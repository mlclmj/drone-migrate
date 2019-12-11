package migrate

import (
	"bytes"
	"database/sql"
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/russross/meddler"
	"github.com/sirupsen/logrus"
)

// MigrateLogs migrates the steps from the V0
// database to the V1 database.
func MigrateLogs(source, target *sql.DB, buildId int64) error {
	stepsV0 := []*StepV0{}

	// 1. load all stages from the V0 database.
	err := meddler.QueryAll(source, &stepsV0, stepListQueryLogs, buildId)
	if err != nil {
		return err
	}

	logrus.Infof("migrating %d logs", len(stepsV0))

	// 2. create a database transaction so that we
	// can rollback if the data migration fails.
	tx, err := target.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 3. iterate through the list and convert from
	// the 0.x to the 1.x structure and insert.
	for _, stepV0 := range stepsV0 {
		logsV0 := &LogsV0{}
		err := meddler.QueryRow(source, logsV0, fmt.Sprintf("select * from logs where log_job_id = %d", stepV0.ID))
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			logrus.WithError(err).Warnf("cannot find logs for step: id: %d", stepV0.ID)
			continue
		}

		logsV1 := &LogsV1{
			ID:   logsV0.ProcID,
			Data: logsV0.Data,
		}

		err = meddler.Insert(tx, "logs", logsV1)
		if err != nil {
			logrus.WithError(err).Errorln("migration failed")
			return err
		}
	}

	logrus.Infof("migration complete")
	return tx.Commit()
}

// MigrateLogsS3 migrates the steps from the V0 database to S3.
func MigrateLogsS3(source *sql.DB, bucket, prefix string, buildId int64) error {
	stepsV0 := []*StepV0{}

	// 1. load all stages from the V0 database.
	err := meddler.QueryAll(source, &stepsV0, stepListQueryLogs, buildId)
	if err != nil {
		return err
	}

	logrus.Infof("migrating %d logs", len(stepsV0))

	// 2. create the s3 client
	sess := session.Must(
		session.NewSession(&aws.Config{
			// Endpoint:         aws.String(endpoint),
			// DisableSSL:       aws.Bool(disableSSL),
			// S3ForcePathStyle: aws.Bool(pathStyle),
		}),
	)

	// 3. iterate through the list and convert from
	// the 0.x to the 1.x structure and insert.
	for i, stepV0 := range stepsV0 {
		logsV0 := &LogsV0{}
		err := meddler.QueryRow(source, logsV0, fmt.Sprintf("select * from logs where log_job_id = %d", stepV0.ID))
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			logrus.WithError(err).Warnf("cannot find logs for step: id: %d", stepV0.ID)
			continue
		}
		if len(logsV0.Data) == 0 {
			logrus.WithError(err).Warnf("skipping empty logs for step: id: %d", stepV0.ID)
			continue
		}

		logrus.Debugf("uploading logs for step: %d", stepV0.ID)

		uploader := s3manager.NewUploader(sess)
		input := &s3manager.UploadInput{
			ACL:    aws.String("private"),
			Bucket: aws.String(bucket),
			Key:    aws.String(s3key(prefix, logsV0.ProcID)),
			Body:   bytes.NewBuffer(logsV0.Data),
		}
		_, err = uploader.Upload(input)
		if err != nil {
			logrus.WithError(err).Errorln("migration failed")
			return err
		}
		if i%1000 == 0 {
			logrus.Infof("uploaded: %d", stepV0.ID)
		}
	}

	logrus.Infof("migration complete")
	return nil
}

func s3key(prefix string, step int64) string {
	return path.Join("/", prefix, fmt.Sprint(step))
}

const stepListQueryLogs = `
SELECT procs.*
FROM procs
INNER JOIN builds ON procs.proc_build_id = builds.build_id
INNER JOIN repos ON builds.build_repo_id = repos.repo_id
WHERE proc_ppid != 0
  AND repo_user_id > 0
	AND builds.build_id > ?
ORDER BY proc_id ASC
`
