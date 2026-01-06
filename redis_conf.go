package redis

import (
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type RedisConf struct {
	DefaultSettings *DefaultSettingsConf  `json:"defaultSettings" mapstructure:"defaultsettings"`
	ResponseStreams []*ResponseStreamConf `json:"responseStreams" mapstructure:"responsestreams"`
	Pool            []*PoolClientConf     `json:"pool" mapstructure:"pool"`
}

func (conf *RedisConf) Validate() error {
	err := validation.ValidateStruct(conf,
		validation.Field(&conf.DefaultSettings, validation.Required),
		validation.Field(&conf.ResponseStreams, validation.Required),
		validation.Field(&conf.Pool),
	)
	if err != nil {
		return err
	}

	return nil
}

type ResponseStreamConf struct {
	Service               string  `json:"service"`
	Stream                *string `json:"stream"`
	Group                 *string `json:"group"`
	ResponseListenerCount int     `json:"responseListenerCount"` // default 1
}

func (conf *ResponseStreamConf) Validate() error {
	err := validation.ValidateStruct(conf,
		validation.Field(&conf.Service, validation.Required),
		validation.Field(&conf.Stream),
		validation.Field(&conf.Group),
		validation.Field(&conf.ResponseListenerCount),
	)
	if err != nil {
		return err
	}
	return nil
}

type DefaultSettingsConf struct {
	Host     string  `json:"host"`
	Port     int     `json:"port"`
	Password *string `json:"password"`
}

func (conf *DefaultSettingsConf) Validate() error {
	err := validation.ValidateStruct(conf,
		validation.Field(&conf.Host, validation.Required),
		validation.Field(&conf.Port, validation.Required),
		validation.Field(&conf.Password),
	)
	if err != nil {
		return err
	}
	return nil
}

type PoolClientConf struct {
	Host     string  `json:"host"`
	Port     int     `json:"port"`
	Password *string `json:"password"`
}

func (conf *PoolClientConf) Validate() error {
	err := validation.ValidateStruct(conf,
		validation.Field(&conf.Host, validation.Required),
		validation.Field(&conf.Port, validation.Required),
		validation.Field(&conf.Password),
	)
	if err != nil {
		return err
	}
	return nil
}
