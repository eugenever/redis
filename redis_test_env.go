package redis

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/redis/rueidis"
	"github.com/spf13/viper"
)

const ENV_FILE = "test.env"

func testEnvFile(fileName string) (string, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	return pathEnvFile(fileName, currentDir)
}

func pathEnvFile(fileName, dir string) (string, error) {
	filePath := filepath.Join(dir, fileName)

	if _, err := os.Stat(filePath); err == nil {
		return filePath, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}

	parentDir := filepath.Dir(dir)

	if parentDir == dir {
		return "", fmt.Errorf("file not found")
	}

	return pathEnvFile(fileName, parentDir)
}

func RedisClientForTest() (rueidis.Client, error) {
	envFile, err := testEnvFile(ENV_FILE)
	if err != nil {
		return nil, err
	}

	viper.SetDefault("TEST_REDIS_HOST", "127.0.0.1")
	viper.SetDefault("TEST_REDIS_PORT", 6379)

	viper.SetConfigFile(envFile)
	viper.ReadInConfig()
	viper.AutomaticEnv()

	host := viper.GetString("TEST_REDIS_HOST")
	if host == "" {
		return nil, errors.New("environment variable 'TEST_REDIS_HOST' not defined")
	}
	port := viper.GetInt("TEST_REDIS_PORT")
	if port == 0 {
		return nil, errors.New("environment variable 'TEST_REDIS_PORT' not defined")
	}
	password := viper.GetString("TEST_REDIS_PASSWORD")

	client, err := NewRedisClient(host, port, &password)
	if err != nil {
		return nil, err
	}
	return client, nil
}
