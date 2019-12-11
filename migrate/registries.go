package migrate

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/russross/meddler"
	"github.com/sirupsen/logrus"
)

// MigrateRegistries migrates the registry crendeitals
// from the V0 database to the V1 database.
func MigrateRegistries(source, target *sql.DB) error {
	registriesV0 := []*RegistryV0{}
	dockerConfigs := make(map[string]DockerConfig, 0)

	if err := meddler.QueryAll(source, &registriesV0, registryImportQuery); err != nil {
		return err
	}

	logrus.Infof("migrating %d registries", len(registriesV0))
	tx, err := target.Begin()

	if err != nil {
		return err
	}

	defer tx.Rollback()

	for _, registryV0 := range registriesV0 {
		log := logrus.WithFields(logrus.Fields{
			"repo": registryV0.RepoFullname,
			"addr": registryV0.Addr,
		})

		log.Debugln("prepare registry")

		if _, ok := dockerConfigs[registryV0.RepoFullname]; !ok {
			dockerConfigs[registryV0.RepoFullname] = DockerConfig{
				AuthConfigs: make(map[string]AuthConfig, 0),
			}
		}

		dockerConfigs[registryV0.RepoFullname].AuthConfigs[registryV0.Addr] = AuthConfig{
			Username: registryV0.Username,
			Password: registryV0.Password,
			Email:    registryV0.Email,
		}

		log.Debugln("prepare complete")
	}

	for repoFullname, dockerConfig := range dockerConfigs {
		log := logrus.WithFields(logrus.Fields{
			"repo": repoFullname,
		})

		log.Debugln("migrate registry")

		result, err := json.Marshal(dockerConfig)

		if err != nil {
			log.WithError(err).Errorln("failed to build docker config")
			continue
		}

		repoV1 := &RepoV1{}

		if err := meddler.QueryRow(target, repoV1, fmt.Sprintf(repoSlugQuery, repoFullname)); err != nil {
			log.WithError(err).Errorln("failed to get registry repo")
			continue
		}

		registryV1 := &RegistryV1{
			RepoID:      repoV1.ID,
			Name:        ".dockerconfigjson",
			Data:        string(result),
			PullRequest: true,
		}

		// row, err := tx.QueryRow("SELECT COUNT(*) FROM secrets WHERE secret_repo_id = ? AND secret_name = ?", registryV1.RepoId, registryV1.Name)
		// if err != nil {
		// 	log.WithError(err).Errorln("error checking for existing registry secret")
		// }
		// var count int64
		// if err := row.Scan(&count); err != nil {
		// 	log.WithError(err).Errorln("error getting count of existing registry secrets")
		// }
		//
		// // This is BAD
		// if count > 1 {
		// 	err = errors.New("duplicate .dockerconfigjson secrets exist for this repo in the target db")
		// 	log.WithError(err).Errorln("registry migration failed due to data issue")
		// 	return err
		// }
		//
		// if count > 0 {
		// 	// meddler's update is too much work to use here
		// 	result, err := tx.Exec(
		// 		"UPDATE secrets SET secret_data = ? WHERE secret_name = ? AND secret_repo_id = ? LIMIT 1",
		// 		registryV1.Data,
		// 		registryV1.Name,
		// 		registryV1.RepoID
		// 	)
		// 	if err != nil {
		// 		log.WithError(err).Errorln("error updating existing registry configuration")
		// 		return err
		// 	}
		// 	rows, err := result.RowsAffected()
		// 	if err != nil {
		// 		log.WithError(err).Errorln("couldn't get resulting rows")
		// 	} else if rows == 1 {
		// 		log.Debugln("successfully updated existing secret")
		// 	}
		// }

		var insert bool
		existing := &RegistryV1{}
		err = meddler.QueryRow(tx, existing, registryFindExistingQuery, registryV1.RepoID, registryV1.Name)
		if err != nil && err.Error() == "sql: no rows in result set" {
			// perform an insert if we didn't find an existing secret for this repo
			insert = true
		} else if err != nil {
			log.WithError(err).Errorln("error querying for existing registry credentials")
			return err
		}

		if insert {
			log.Debugln("inserting new registry secret")
			if err := meddler.Insert(tx, "secrets", registryV1); err != nil {
				log.WithError(err).Errorln("failed to insert new registry credential secret")
				return err
			}
		} else {
			log.Debugln("updating existing registry secret")
			// we're just updating the data value of the existing registry secret in case it changed
			existing.Data = registryV1.Data
			if err := meddler.Update(tx, "secrets", existing); err != nil {
				log.WithError(err).Errorln("failed to update exisitng registry credential secret")
				return err
			}
		}


		log.Debugln("migration complete")
	}

	logrus.Infof("migration complete")
	return tx.Commit()
}

const registryImportQuery = `
SELECT
	repo_full_name,
	registry.*
FROM registry INNER JOIN repos ON (repo_id = registry_repo_id)
WHERE repo_user_id > 0
`

const registryFindExistingQuery = `
SELECT * FROM secrets WHERE secret_repo_id = ? AND secret_name = '?'
`
