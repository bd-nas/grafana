package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	"github.com/grafana/grafana/pkg/plugins/backendplugin"
	"github.com/grafana/grafana/pkg/plugins/backendplugin/coreplugin"
	"github.com/grafana/grafana/pkg/registry"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/util/errutil"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/tsdb/sqleng"
)

// type datasourceInfo struct {
// 	datasourceID           int64
// 	timescaledb            bool
// 	uid                    string
// 	database               string
// 	url                    string
// 	user                   string
// 	password               string
// 	sslmode                string
// 	tlsConfigurationMethod string
// 	tlsCACert              string
// 	tlsClientCert          string
// 	tlsClientKey           string
// 	sslRootCertFile        string
// 	sslCertFile            string
// 	sslKeyFile             string
// 	updated                time.Time
// }

type postgresExecutor struct {
	im         instancemgmt.InstanceManager
	cfg        *setting.Cfg
	logger     *log.Logger
	tlsManager tlsSettingsProvider
}

func init() {
	registry.Register(&registry.Descriptor{
		Name:         "PostgresService",
		InitPriority: registry.Low,
		Instance:     &PostgresService{},
	})
}

type PostgresService struct {
	Cfg                  *setting.Cfg `inject:""`
	logger               log.Logger
	BackendPluginManager backendplugin.Manager `inject:""`
}

func NewInstanceSettings() datasource.InstanceFactoryFunc {
	return func(settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
		type JSONData struct {
			SSLMode                string `json:"sslmode"`
			TLSConfigurationMethod string `json:"tlsConfigurationMethod"`
			Timescaledb            bool   `json:"timescaledb"`
			SSLRootCertFile        string `json:"sslRootCertFile"`
			SSLCertFile            string `json:"sslCertFile"`
			SSLKeyFile             string `json:"sslKeyFile"`
		}

		// set up defaults
		jsonData := JSONData{
			SSLMode:                "verify-full",
			TLSConfigurationMethod: "file-path",
			Timescaledb:            false,
		}

		err := json.Unmarshal(settings.JSONData, &jsonData)
		if err != nil {
			return nil, fmt.Errorf("error reading settings: %w", err)
		}

		model := sqleng.DatasourceInfo{
			datasourceID:           settings.ID,
			uid:                    settings.UID,
			url:                    settings.URL,
			user:                   settings.User,
			database:               settings.Database,
			updated:                settings.Updated,
			sslmode:                strings.TrimSpace(strings.ToLower(jsonData.SSLMode)),
			tlsConfigurationMethod: strings.TrimSpace(strings.ToLower(jsonData.TLSConfigurationMethod)),
			timescaledb:            jsonData.Timescaledb,
			sslRootCertFile:        jsonData.SSLRootCertFile,
			sslCertFile:            jsonData.SSLCertFile,
			sslKeyFile:             jsonData.SSLKeyFile,
		}

		model.password = settings.DecryptedSecureJSONData["password"]
		model.tlsCACert = settings.DecryptedSecureJSONData["tlsCACert"]
		model.tlsClientCert = settings.DecryptedSecureJSONData["tlsClientCert"]
		model.tlsClientKey = settings.DecryptedSecureJSONData["tlsClientKey"]

		return model, nil
	}
}

func (s *PostgresService) Init() error {
	s.logger = log.New("tsdb.postgres")
	im := datasource.NewInstanceManager(NewInstanceSettings())
	factory := coreplugin.New(backend.ServeOpts{
		QueryDataHandler: newExecutor(im, s.Cfg, &s.logger),
	})

	if err := s.BackendPluginManager.Register("postgres", factory); err != nil {
		s.logger.Error("Failed to register plugin", "error", err)
	}
	return nil
}

func newExecutor(im instancemgmt.InstanceManager, cfg *setting.Cfg, logger *log.Logger) backend.QueryDataHandler {
	return &postgresExecutor{
		im:         im,
		cfg:        cfg,
		logger:     logger,
		tlsManager: newTLSManager(*logger, cfg.DataPath),
	}
}

func (e *postgresExecutor) getDSInfo(pluginCtx backend.PluginContext) (*datasourceInfo, error) {
	i, err := e.im.Get(pluginCtx)
	if err != nil {
		return nil, err
	}
	instance := i.(datasourceInfo)
	return &instance, nil
}

func (e *postgresExecutor) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	(*e.logger).Debug("Creating Postgres query endpoint")
	dsInfo, err := e.getDSInfo(req.PluginContext)
	if err != nil {
		return nil, err
	}

	cnnstr, err := e.generateConnectionString(dsInfo)

	if e.cfg.Env == setting.Dev {
		(*e.logger).Debug("getEngine", "connection", cnnstr)
	}

	config := sqleng.DataPluginConfiguration{
		DriverName:        "postgres",
		ConnectionString:  cnnstr,
		Datasource:        dsInfo,
		MetricColumnTypes: []string{"UNKNOWN", "TEXT", "VARCHAR", "CHAR"},
	}

	queryResultTransformer := postgresQueryResultTransformer{
		log: *e.logger,
	}

	plugin, err := sqleng.NewQueryDataHandler(config, &queryResultTransformer, newPostgresMacroEngine(dsInfo.timescaledb),
		*e.logger)
	if err != nil {
		(*e.logger).Error("Failed connecting to Postgres", "err", err)
		return nil, err
	}

	(*e.logger).Debug("Successfully connected to Postgres")
	return &plugin, nil
}

// escape single quotes and backslashes in Postgres connection string parameters.
func escape(input string) string {
	return strings.ReplaceAll(strings.ReplaceAll(input, `\`, `\\`), "'", `\'`)
}

func (s *postgresExecutor) generateConnectionString(dsInfo *datasourceInfo) (string, error) {
	var host string
	var port int
	if strings.HasPrefix(dsInfo.url, "/") {
		host = dsInfo.url
		(*s.logger).Debug("Generating connection string with Unix socket specifier", "socket", host)
	} else {
		sp := strings.SplitN(dsInfo.url, ":", 2)
		host = sp[0]
		if len(sp) > 1 {
			var err error
			port, err = strconv.Atoi(sp[1])
			if err != nil {
				return "", errutil.Wrapf(err, "invalid port in host specifier %q", sp[1])
			}

			(*s.logger).Debug("Generating connection string with network host/port pair", "host", host, "port", port)
		} else {
			(*s.logger).Debug("Generating connection string with network host", "host", host)
		}
	}

	connStr := fmt.Sprintf("user='%s' password='%s' host='%s' dbname='%s'",
		escape(dsInfo.user), escape(dsInfo.password), escape(host), escape(dsInfo.database))
	if port > 0 {
		connStr += fmt.Sprintf(" port=%d", port)
	}

	tlsSettings, err := s.tlsManager.getTLSSettings(dsInfo)
	if err != nil {
		return "", err
	}

	connStr += fmt.Sprintf(" sslmode='%s'", escape(tlsSettings.Mode))

	// Attach root certificate if provided
	if tlsSettings.RootCertFile != "" {
		(*s.logger).Debug("Setting server root certificate", "tlsRootCert", tlsSettings.RootCertFile)
		connStr += fmt.Sprintf(" sslrootcert='%s'", escape(tlsSettings.RootCertFile))
	}

	// Attach client certificate and key if both are provided
	if tlsSettings.CertFile != "" && tlsSettings.CertKeyFile != "" {
		(*s.logger).Debug("Setting TLS/SSL client auth", "tlsCert", tlsSettings.CertFile, "tlsKey", tlsSettings.CertKeyFile)
		connStr += fmt.Sprintf(" sslcert='%s' sslkey='%s'", escape(tlsSettings.CertFile), escape(tlsSettings.CertKeyFile))
	} else if tlsSettings.CertFile != "" || tlsSettings.CertKeyFile != "" {
		return "", fmt.Errorf("TLS/SSL client certificate and key must both be specified")
	}

	(*s.logger).Debug("Generated Postgres connection string successfully")
	return connStr, nil
}

type postgresQueryResultTransformer struct {
	log log.Logger
}

func (t *postgresQueryResultTransformer) TransformQueryError(err error) error {
	return err
}

func (t *postgresQueryResultTransformer) GetConverterList() []sqlutil.StringConverter {
	return []sqlutil.StringConverter{
		{
			Name:           "handle FLOAT4",
			InputScanKind:  reflect.Interface,
			InputTypeName:  "FLOAT4",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableFloat64,
				ReplaceFunc: func(in *string) (interface{}, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseFloat(*in, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
		{
			Name:           "handle FLOAT8",
			InputScanKind:  reflect.Interface,
			InputTypeName:  "FLOAT8",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableFloat64,
				ReplaceFunc: func(in *string) (interface{}, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseFloat(*in, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
		{
			Name:           "handle NUMERIC",
			InputScanKind:  reflect.Interface,
			InputTypeName:  "NUMERIC",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableFloat64,
				ReplaceFunc: func(in *string) (interface{}, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseFloat(*in, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
	}
}
