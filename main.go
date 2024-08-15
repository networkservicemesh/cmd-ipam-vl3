// Copyright (c) 2022-2023 Cisco and/or its affiliates.
//
// Copyright (c) 2024 OpenInfra Foundation Europe. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/networkservicemesh/api/pkg/api/ipam"
	"github.com/networkservicemesh/sdk/pkg/ipam/vl3ipam"
	"github.com/networkservicemesh/sdk/pkg/tools/grpcutils"
	"github.com/networkservicemesh/sdk/pkg/tools/log"
	"github.com/networkservicemesh/sdk/pkg/tools/log/logruslogger"
	"github.com/networkservicemesh/sdk/pkg/tools/opentelemetry"
	"github.com/networkservicemesh/sdk/pkg/tools/pprofutils"
	"github.com/networkservicemesh/sdk/pkg/tools/tracing"
)

// Config represents the configuration for cmd-map-ip-k8s application
type Config struct {
	ListenOn              []url.URL     `default:"unix:///listen.on.socket" desc:"url to listen on." split_words:"true"`
	LogLevel              string        `default:"INFO" desc:"Log level" split_words:"true"`
	OpenTelemetryEndpoint string        `default:"otel-collector.observability.svc.cluster.local:4317" desc:"OpenTelemetry Collector Endpoint" split_words:"true"`
	MetricsExportInterval time.Duration `default:"10s" desc:"interval between mertics exports" split_words:"true"`
	Prefix                string        `default:"169.254.0.0/16" desc:"CIDR Prefix to allocate CIDR prefixes for clients" split_words:"true"`
	ClientPrefixLen       uint8         `default:"24" desc:"Default len of clients prefix" split_words:"true"`
	PprofEnabled          bool          `default:"false" desc:"is pprof enabled" split_words:"true"`
	PprofListenOn         string        `default:"localhost:6060" desc:"pprof URL to ListenAndServe" split_words:"true"`
}

func main() {
	// ********************************************************************************
	// Configure signal handling context
	// ********************************************************************************
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		// More Linux signals here
		syscall.SIGHUP,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	defer cancel()

	// ********************************************************************************
	// Setup logger
	// ********************************************************************************
	log.EnableTracing(true)
	logrus.SetFormatter(&nested.Formatter{})
	ctx = log.WithLog(ctx, logruslogger.New(ctx, map[string]interface{}{"cmd": os.Args[:1]}))

	logger := log.FromContext(ctx)

	// ********************************************************************************
	// Get config from environment
	// ********************************************************************************
	config := &Config{}
	if err := envconfig.Usage("nsm", config); err != nil {
		logger.Fatal(err)
	}
	if err := envconfig.Process("nsm", config); err != nil {
		logger.Fatalf("error processing rootConf from env: %+v", err)
	}
	logger.Infof("Configuration: %v", config)

	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		logrus.Fatalf("invalid log level %s", config.LogLevel)
	}
	logrus.SetLevel(level)

	// ********************************************************************************
	// Configure Open Telemetry
	// ********************************************************************************
	if opentelemetry.IsEnabled() {
		collectorAddress := config.OpenTelemetryEndpoint
		spanExporter := opentelemetry.InitSpanExporter(ctx, collectorAddress)
		metricExporter := opentelemetry.InitOPTLMetricExporter(ctx, collectorAddress, config.MetricsExportInterval)
		o := opentelemetry.Init(ctx, spanExporter, metricExporter, os.Args[0])
		defer func() {
			if err = o.Close(); err != nil {
				logger.Error(err.Error())
			}
		}()
	}

	// ********************************************************************************
	// Configure pprof
	// ********************************************************************************
	if config.PprofEnabled {
		go pprofutils.ListenAndServe(ctx, config.PprofListenOn)
	}

	source, err := workloadapi.NewX509Source(ctx)
	if err != nil {
		logrus.Fatalf("error getting x509 source: %+v", err)
	}
	svid, err := source.GetX509SVID()
	if err != nil {
		logrus.Fatalf("error getting x509 svid: %+v", err)
	}
	log.FromContext(ctx).Infof("SVID: %q", svid.ID)
	options := append(
		tracing.WithTracing(),
		grpc.Creds(
			credentials.NewTLS(
				tlsconfig.MTLSServerConfig(source, source, tlsconfig.AuthorizeAny()),
			),
		),
	)
	server := grpc.NewServer(options...)

	ipam.RegisterIPAMServer(server, vl3ipam.NewIPAMServer(config.Prefix, config.ClientPrefixLen))

	for i := 0; i < len(config.ListenOn); i++ {
		srvErrCh := grpcutils.ListenAndServe(ctx, &config.ListenOn[i], server)
		exitOnErr(ctx, cancel, srvErrCh)
	}

	<-ctx.Done()
}

func exitOnErr(ctx context.Context, cancel context.CancelFunc, errCh <-chan error) {
	// If we already have an error, log it and exit
	select {
	case err := <-errCh:
		log.FromContext(ctx).Fatal(err)
	default:
	}
	// Otherwise wait for an error in the background to log and cancel
	go func(ctx context.Context, errCh <-chan error) {
		err := <-errCh
		log.FromContext(ctx).Error(err)
		cancel()
	}(ctx, errCh)
}
