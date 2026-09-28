#!/bin/sh
# Releases the Go SDK from this Mac: copies sdks/go, with its history, to the
# public repository behind withruntime.com/go and tags the version there. Run
# it from anywhere:
#
#   sdks/go/scripts/release.sh git@github.com:OWNER/REPO.git
#
# The steps before the first run, and the ones after, are in sdks/go/RELEASE.md.
# Bump Version in client.go first; a tag that exists is refused.
set -eu

remote=${1:?usage: release.sh <public git remote of withruntime.com/go>}
here=$(cd "$(dirname "$0")/.." && pwd)
top=$(git -C "$here" rev-parse --show-toplevel)
version=v$(sed -n 's/^const Version = "\(.*\)"/\1/p' "$here/client.go")

if [ -n "$(git -C "$top" status --porcelain -- sdks/go)" ]; then
  echo "sdks/go has uncommitted changes; release a commit." >&2
  exit 1
fi
(cd "$here" && gofmt -l . | grep . && { echo "gofmt would change the files above." >&2; exit 1; } || true)
(cd "$here" && go vet ./... && go test ./...)

branch="go-sdk-$version"
git -C "$top" subtree split --prefix=sdks/go -b "$branch"
git -C "$top" push "$remote" "$branch:main"
git -C "$top" push "$remote" "$branch:refs/tags/$version"
git -C "$top" branch -D "$branch"

# Ask the Go module proxy for the new version, so `go get` and pkg.go.dev see it now.
GOPROXY=https://proxy.golang.org GOFLAGS=-mod=mod go list -m "withruntime.com/go@$version"
echo "Released withruntime.com/go $version."
