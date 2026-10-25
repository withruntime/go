package withruntime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type walletSeen struct {
	path          string
	body          map[string]any
	authorization string
}

func TestWalletOpenAndClaim(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var seen []walletSeen
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		mu.Lock()
		seen = append(seen, walletSeen{r.URL.Path, body, r.Header.Get("Authorization")})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/claim") {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"That claim code opens no account."}`))
			return
		}
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"purchaseId":"p1","status":"pending","amountUsd":"20.00","amount":"20.000000","payBy":null,` +
			`"networks":[{"network":"base","address":"0xabc","tokens":[{"currency":"USDC","contract":"0xdef"}]}],` +
			`"orgId":"o1","claimCode":"rtclaim_x","next":"Send it."}`))
	}))
	defer site.Close()

	opened, err := OpenWalletAccount(context.Background(), 20, true, WithWalletAuthURL(site.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}
	if opened.ClaimCode != "rtclaim_x" || opened.Amount != "20.000000" || opened.PayBy != nil ||
		opened.Networks[0].Address != "0xabc" || opened.Networks[0].Tokens[0].Currency != "USDC" {
		t.Fatalf("opened = %+v", opened)
	}
	_, err = ClaimWalletAccount(context.Background(), "rtclaim_nope", WithWalletAuthURL(site.URL))
	var refused *Error
	if !errors.As(err, &refused) || refused.Status != 404 || refused.Code != "request_failed" ||
		refused.Message != "That claim code opens no account." {
		t.Fatalf("err = %v", err)
	}
	want := []walletSeen{
		{"/api/wallet/topups", map[string]any{"usd": float64(20), "acceptTerms": true}, ""},
		{"/api/wallet/claim", map[string]any{"claimCode": "rtclaim_nope"}, ""},
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("seen = %+v, want %+v", seen, want)
	}
}

func TestWalletClaimNotPaid(t *testing.T) {
	t.Parallel()
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
		_, _ = w.Write([]byte(`{"status":"open","orgId":"o1","next":"Pay, then call again.",` +
			`"topup":{"purchaseId":"p1","status":"open","amountUsd":"20.00","amount":"20.000000",` +
			`"payBy":"2026-10-15T00:00:00Z","networks":[]}}`))
	}))
	defer site.Close()
	claim, err := ClaimWalletAccount(context.Background(), "rtclaim_x", WithWalletAuthURL(site.URL))
	if err != nil {
		t.Fatal(err)
	}
	if claim.Status != "open" || claim.APIKey != "" || claim.Next != "Pay, then call again." ||
		claim.Topup == nil || claim.Topup.PayBy == nil || *claim.Topup.PayBy != "2026-10-15T00:00:00Z" {
		t.Fatalf("claim = %+v", claim)
	}
}

func TestWalletNoAnswer(t *testing.T) {
	t.Parallel()
	site := httptest.NewServer(http.NotFoundHandler())
	url := site.URL
	site.Close()
	_, err := OpenWalletAccount(context.Background(), 20, true, WithWalletAuthURL(url))
	var failed *Error
	if !errors.As(err, &failed) || failed.Code != "network" || failed.Hint != "Retry in a minute." {
		t.Fatalf("err = %v", err)
	}
}
