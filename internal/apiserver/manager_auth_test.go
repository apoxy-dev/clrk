package apiserver

import (
	"context"
	"net/http/httptest"
	"testing"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	apiserver "k8s.io/apiserver/pkg/server"
)

func TestConfigureOpenAccess(t *testing.T) {
	cfg := configureOpenAccess(&apiserver.RecommendedConfig{})
	request := httptest.NewRequest("GET", "https://127.0.0.1/apis/clrk.apoxy.dev/v1alpha1/taskagents", nil)

	response, authenticated, err := cfg.Authentication.Authenticator.AuthenticateRequest(request)
	if err != nil {
		t.Fatalf("authenticate request: %v", err)
	}
	if !authenticated {
		t.Fatal("request was not authenticated as anonymous")
	}
	if got := response.User.GetName(); got != user.Anonymous {
		t.Fatalf("user name = %q, want %q", got, user.Anonymous)
	}

	decision, reason, err := cfg.Authorization.Authorizer.Authorize(context.Background(), authorizer.AttributesRecord{
		User:            response.User,
		Verb:            "watch",
		APIGroup:        "clrk.apoxy.dev",
		APIVersion:      "v1alpha1",
		Resource:        "taskagents",
		ResourceRequest: true,
	})
	if err != nil {
		t.Fatalf("authorize request: %v", err)
	}
	if decision != authorizer.DecisionAllow {
		t.Fatalf("authorization decision = %v, want allow; reason: %s", decision, reason)
	}
}
