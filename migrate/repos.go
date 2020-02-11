package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/dchest/uniuri"
	"github.com/drone/drone-go/drone"
	"github.com/drone/go-scm/scm"
	"github.com/hashicorp/go-multierror"
	"github.com/russross/meddler"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
)

// MigrateRepos migrates the repositories from the V0
// database to the V1 database.
func MigrateRepos(source, target *sql.DB) error {
	reposV0 := []*RepoV0{}

	if err := meddler.QueryAll(source, &reposV0, repoImportQuery); err != nil {
		return err
	}

	logrus.Infof("migrating %d repositories", len(reposV0))

	tx, err := target.Begin()

	if err != nil {
		return err
	}

	defer tx.Rollback()

	var sequence int64
	for _, repoV0 := range reposV0 {
		if repoV0.ID > sequence {
			sequence = repoV0.ID
		}

		log := logrus.WithFields(logrus.Fields{
			"id":   repoV0.ID,
			"repo": repoV0.FullName,
		})

		log.Debugln("migrate repository")

		if repoV0.Owner == "NYTimes" {
			log.Debugln("lowercasing repo: " + repoV0.FullName)
			repoV0.Owner = "nytimes"
			repoV0.FullName = "nytimes/" + repoV0.Name
		}

		repoV1 := &RepoV1{
			ID:         repoV0.ID,
			UserID:     repoV0.UserID,
			Namespace:  repoV0.Owner,
			Name:       repoV0.Name,
			Slug:       repoV0.FullName,
			SCM:        "git",
			HTTPURL:    repoV0.Clone,
			SSHURL:     "",
			Link:       repoV0.Link,
			Branch:     repoV0.Branch,
			Private:    repoV0.IsPrivate,
			Visibility: repoV0.Visibility,
			Active:     repoV0.IsActive,
			Config:     repoV0.Config,
			Trusted:    repoV0.IsTrusted,
			Protected:  repoV0.IsGated,
			Timeout:    repoV0.Timeout,
			Counter:    int64(repoV0.Counter),
			Synced:     time.Now().Unix(),
			Created:    time.Now().Unix(),
			Updated:    time.Now().Unix(),
			Version:    1,
			Signer:     uniuri.NewLen(32),
			Secret:     uniuri.NewLen(32),

			// We use a temporary repository identifier here.
			// We need to do a per-repository lookup to get the
			// actual identifier from the source code management
			// system.
			UID: fmt.Sprintf("temp_%d", repoV0.ID),
		}

		// if the repository is activate and pull requests
		// are disabled in the source database, configure the
		// target repository to ignore pull requests.
		if repoV0.AllowPull == false && repoV0.UserID > 0 {
			repoV1.IgnorePulls = true
		}

		var insert bool
		err = meddler.QueryRow(tx, &RepoV1{}, fmt.Sprintf("SELECT * FROM repos WHERE repo_id = %d", repoV1.ID))
		if err != nil && err.Error() == "sql: no rows in result set" {
			insert = true
		} else if err != nil {
			log.WithError(err).Errorln("error querying for existing repo")
			return err
		}

		if insert {
			log.Debugln("inserting new repo")
			if err := meddler.Insert(tx, "repos", repoV1); err != nil {
				log.WithError(err).Errorln("failed to insert new repo")
				return err
			}
		} else {
			log.Debugln("updating existing repo")
			update := (*RepoV1Update)(repoV1)
			if err := meddler.Update(tx, "repos", update); err != nil {
				log.WithError(err).Errorln("failed to update existing repo")
				return err
			}
		}

		log.Debugln("migration complete")
	}

	if meddler.Default == meddler.PostgreSQL {
		_, err = tx.Exec(fmt.Sprintf(updateRepoSeq, sequence+1))
		if err != nil {
			logrus.WithError(err).Errorln("failed to reset sequence")
			return err
		}
	}

	logrus.Infoln("migration complete")
	return tx.Commit()
}

// UpdateRepoIdentifiers updates the repository identifiers
// from temporary values (assigned during migration) to the
// value fetched from the source code management system.
func UpdateRepoIdentifiers(db *sql.DB, client *scm.Client, overrideToken string) error {
	repos := []*RepoV1{}
	var result error

	if err := meddler.QueryAll(db, &repos, repoTempQuery); err != nil {
		return err
	}

	logrus.Infoln("updating repository metadata")

	for _, repo := range repos {
		log := logrus.WithFields(logrus.Fields{
			"repo": repo.Slug,
		})

		user := &UserV1{}

		if err := meddler.QueryRow(db, user, fmt.Sprintf(userIdentifierQuery, repo.UserID)); err != nil {
			log.WithError(err).Errorf("failed to get repository owner")
			multierror.Append(result, err)
			continue
		}

		var tok *scm.Token

		if overrideToken != "" {
			log.Infof("using an override token from the environment to make requests")
			tok = &scm.Token{
				Token:   overrideToken,
			}
		} else {
			log = log.WithField("owner", user.Login)
			tok := &scm.Token{
				Token:   user.Token,
				Refresh: user.Refresh,
			}
			if user.Expiry > 0 {
				tok.Expires = time.Unix(user.Expiry, 0)
			}
		}


		ctx := scm.WithContext(context.Background(), tok)

		remoteRepo, _, err := client.Repositories.Find(ctx, scm.Join(repo.Namespace, repo.Name))

		if err != nil {
			log.WithError(err).Errorf("failed to get remote repository")
			multierror.Append(result, err)
			continue
		}

		if remoteRepo.Name != repo.Name || remoteRepo.Namespace != repo.Namespace {
			log.Warnf("Skipping repo since the name doesn't match! remote: %s/%s local: %s/%s", remoteRepo.Namespace, remoteRepo.Name, repo.Namespace, repo.Name)
			continue
		}

		if _, err := db.Exec(fmt.Sprintf(repoUpdateQuery, remoteRepo.ID, repo.ID)); err != nil {
			log.WithError(err).Errorf("failed to update metadata with uid %s and repo id %d", remoteRepo.ID, repo.ID)
			multierror.Append(result, err)
		}

		log.Debugln("updated metadata")
	}

	logrus.Infoln("repository metadata update complete")
	return result
}

// ActivateRepositories re-activates the repositories.
// This will create new webhooks and populate any empty
// values (security keys, etc).
func ActivateRepositories(db *sql.DB, client drone.Client) error {
	repos := []*RepoV1{}
	var result error

	if err := meddler.QueryAll(db, &repos, repoActivateQuery); err != nil {
		return err
	}

	logrus.Infoln("begin repository activation")

	for _, repo := range repos {
		log := logrus.WithFields(logrus.Fields{
			"repo": repo.Slug,
		})
		if !repo.Active {
			// https://discourse.drone.io/t/drone-migrates-repoactivatequery-is-invalid/5156
			continue
		}

		log.Debugln("activating repository")

		user := &UserV1{}

		if err := meddler.QueryRow(db, user, fmt.Sprintf(userIdentifierQuery, repo.UserID)); err != nil {
			log.WithError(err).Errorf("failed to get repository owner")
			multierror.Append(result, err)
			continue
		}

		log = log.WithField("owner", user.Login)

		config := new(oauth2.Config)

		client.SetClient(config.Client(
			oauth2.NoContext,
			&oauth2.Token{
				AccessToken: user.Hash,
			},
		))

		if _, err := client.RepoPost(repo.Namespace, repo.Name); err != nil {
			log.WithError(err).Errorf("activation failed")
			multierror.Append(result, err)
			continue
		}

		log.Debugln("successfully activated")
	}

	logrus.Infoln("repository activation complete")
	return result
}

func ActivateReposPreflight(db *sql.DB, client drone.Client) error {
	repos := []*RepoV1{}
	var result error

	if err := meddler.QueryAll(db, &repos, repoActivateQuery); err != nil {
		return err
	}

	logrus.Infoln("begin repository activation preflighting")

	for _, repo := range repos {
		log := logrus.WithFields(logrus.Fields{
			"repo": repo.Slug,
		})
		if !repo.Active {
			// https://discourse.drone.io/t/drone-migrates-repoactivatequery-is-invalid/5156
			continue
		}

		log.Debugln("preflighting repo activation")

		user := &UserV1{}

		if err := meddler.QueryRow(db, user, fmt.Sprintf(userIdentifierQuery, repo.UserID)); err != nil {
			log.WithError(err).Errorf("failed to get repository owner")
			multierror.Append(result, err)
			continue
		}

		log = log.WithField("owner", user.Login)

		hc := &http.Client{}

		req, err := http.NewRequest("GET", fmt.Sprintf("http://api.github.com/repos/%s/%s/readme", repo.Namespace, repo.Name), nil)
		if err != nil {
			log.WithError(err).Errorf("error creating the request")
		}
		req.Header.Add("Authorization", fmt.Sprintf("token %s", user.Token))
		if _, err := hc.Do(req); err != nil {
			log.WithError(err).Errorf("preflight activation failed")
			multierror.Append(result, err)
			continue
		}

		log.Debugln("successfully preflighted activation")
	}

	logrus.Infoln("repository activation preflight complete")
	return result
}

// RemoveRenamed removes repositories that have been renamed
// or cannot be found in the remote system.
func RemoveRenamed(db *sql.DB, client *scm.Client) error {
	repos := []*RepoV1{}
	var result error

	if err := meddler.QueryAll(db, &repos, repoTempQuery); err != nil {
		return err
	}

	logrus.Infoln("removing renamed repositories")

	for _, repo := range repos {
		log := logrus.WithFields(logrus.Fields{
			"repo": repo.Slug,
		})

		user := &UserV1{}

		if err := meddler.QueryRow(db, user, fmt.Sprintf(userIdentifierQuery, repo.UserID)); err != nil {
			log.WithError(err).Errorf("failed to get repository owner")
			multierror.Append(result, err)
			continue
		}

		log = log.WithField("owner", user.Login)

		tok := &scm.Token{
			Token:   user.Token,
			Refresh: user.Refresh,
		}
		if user.Expiry > 0 {
			tok.Expires = time.Unix(user.Expiry, 0)
		}
		ctx := scm.WithContext(context.Background(), tok)

		remoteRepo, _, err := client.Repositories.Find(ctx, scm.Join(repo.Namespace, repo.Name))

		if err != nil {
			log.WithError(err).Errorf("failed to get remote repository")
			multierror.Append(result, err)
			continue
		}

		remoteName := scm.Join(remoteRepo.Namespace, remoteRepo.Name)
		if remoteName == repo.Slug {
			log.Debugln("skip repository, found in remote system")
			continue
		}

		if _, err := db.Exec(fmt.Sprintf(deleteRepo, repo.ID)); err != nil {
			log.WithError(err).Errorf("failed to remove repository")
			multierror.Append(result, err)
		}

		log.WithField("renamed", remoteName).
			Debugln("renamed repository removed")
	}

	logrus.Infoln("repository removal complete")
	return result
}

// RemoveNotFound removes repositories that are not found
// in the remote system.
func RemoveNotFound(db *sql.DB, client *scm.Client) error {
	repos := []*RepoV1{}
	var result error

	if err := meddler.QueryAll(db, &repos, repoTempQuery); err != nil {
		return err
	}

	logrus.Infoln("removing not found repositories")

	for _, repo := range repos {
		log := logrus.WithFields(logrus.Fields{
			"repo": repo.Slug,
		})

		user := &UserV1{}

		if err := meddler.QueryRow(db, user, fmt.Sprintf(userIdentifierQuery, repo.UserID)); err != nil {
			log.WithError(err).Errorf("failed to get repository owner")
			multierror.Append(result, err)
			continue
		}

		log = log.WithField("owner", user.Login)

		tok := &scm.Token{
			Token:   user.Token,
			Refresh: user.Refresh,
		}
		if user.Expiry > 0 {
			tok.Expires = time.Unix(user.Expiry, 0)
		}
		ctx := scm.WithContext(context.Background(), tok)

		_, _, err := client.Repositories.Find(ctx, scm.Join(repo.Namespace, repo.Name))

		if err == nil {
			log.Debugln("skip repository, found in remote system")
			continue
		}

		if _, err := db.Exec(fmt.Sprintf(deleteRepo, repo.ID)); err != nil {
			log.WithError(err).Errorf("failed to remove repository")
			multierror.Append(result, err)
		}

		log.Debugln("not found repository removed")
	}

	logrus.Infoln("repository removal complete")
	return result
}

const repoImportQuery = `
SELECT *
FROM repos
WHERE repo_user_id > 0
`

const repoTempQuery = `
SELECT *
FROM repos
WHERE repo_uid LIKE 'temp_%'
`

const userIdentifierQuery = `
SELECT *
FROM users
WHERE user_id = %d
`

const repoUpdateQuery = `
UPDATE repos
SET repo_uid = '%s'
WHERE repo_id = %d
`

const repoActivateQuery = `
SELECT *
FROM repos
`

const updateRepoSeq = `
ALTER SEQUENCE repos_repo_id_seq
RESTART WITH %d
`

const deleteRepo = `
DELETE FROM repos WHERE repo_id = %d
`
