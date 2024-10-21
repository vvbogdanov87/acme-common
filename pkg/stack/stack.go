package stack

import (
	"context"

	"github.com/pkg/errors"

	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/common/tokens"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func GetStack(ctx context.Context, program pulumi.RunFunc, kind, name, namespace string) (*auto.Stack, error) {
	stackName := namespace + "-" + name

	project := auto.Project(workspace.Project{
		Name:    tokens.PackageName(kind),
		Runtime: workspace.NewProjectRuntimeInfo("go", nil),
		Backend: &workspace.ProjectBackend{
			URL: "s3://acme-cloud-backend",
		},
	})

	// Setup a passphrase secrets provider and use an environment variable to pass in the passphrase.
	secretsProvider := auto.SecretsProvider("passphrase")
	envvars := auto.EnvVars(map[string]string{
		// In a real program, you would feed in the password securely or via the actual environment.
		"PULUMI_CONFIG_PASSPHRASE": "password",
	})

	stackSettings := auto.Stacks(map[string]workspace.ProjectStack{
		stackName: {SecretsProvider: "passphrase"},
	})

	s, err := auto.UpsertStackInlineSource(
		ctx,
		stackName,
		kind,
		program,
		project,
		secretsProvider,
		stackSettings,
		envvars,
	)
	if err != nil {
		// TODO: handle different error types
		return nil, errors.Wrapf(err, "failed to create stack %s", stackName)
	}

	if err := s.SetConfig(ctx, "aws:region", auto.ConfigValue{Value: "us-west-2"}); err != nil {
		return nil, errors.Wrap(err, "failed to set pulumi config")
	}

	return &s, nil
}
