# Releasing the Go SDK

The module path is `withruntime.com/go`. `go get` resolves it by fetching
`https://withruntime.com/go?go-get=1` and reading a `go-import` meta tag that
names a public Git repository whose root is this folder. This repository is
private and holds the SDK in a subfolder, so the module needs a public home of
its own.

## Once, before the first release

1. Make a public, empty GitHub repository for the SDK, for example
   `github.com/withruntime/go`.
2. The vanity path is `apps/cloud-web/src/app/go/[[...path]]/route.ts`.
   `GET /go` (and any path under it) with `?go-get=1` answers the `go-import`
   and `go-source` tags for `GO_REPOSITORY`; without it, it redirects to
   `/docs/go`. It goes live with any site deploy. Until the repository has a
   tag and the guide below is published, `go get` still fails and the redirect
   lands on a 404, so release, publish the guide and deploy together.

## Each release

1. Bump `Version` in `client.go` and commit.
2. Run `sdks/go/scripts/release.sh git@github.com:withruntime/go.git`. It
   runs gofmt, vet and the tests, copies `sdks/go` with its history to the
   public repository's `main`, tags the version there (`v0.1.0`), and asks
   `proxy.golang.org` for it so `go get` and pkg.go.dev see it at once.
3. Check `go get withruntime.com/go@v0.1.0` in an empty module.

## The first release only: publish the guide

`GUIDE.md` is the page for `https://withruntime.com/docs/go`. It stays here
until the module can be installed, so the site never tells a customer to run a
`go get` that fails. With the first release:

1. Move it to `packages/cloud-guide/docs/go.md` and change the path in
   `docs_test.go` to match.
2. Add `["go", "Go"]` back to the SDK links in
   `apps/cloud-web/src/app/docs/navigation.tsx`, and `"go"` to the guide lists
   in `apps/cloud-web/src/app/docs/guide-model.test.ts` and
   `guide-render.test.tsx`.
3. Run `bun run generate` in `packages/cloud-guide`, and say Go in the places
   that list the SDKs (`start.md`, the comparison pages, the landing page).
