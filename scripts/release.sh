#!/bin/sh
# Releases the Go SDK from this Mac: commits the committed sdks/go tree to the
# public repository behind withruntime.com/go, as Runtime (the runtimeteam
# account) and signed with its key, and tags the version there. Run it from
# anywhere:
#
#   sdks/go/scripts/release.sh git@github.com:OWNER/REPO.git
#
# The public history is one commit per release, never this repository's own
# commits: they carry personal authors and messages about other work (Marc,
# 28 September 2026). The steps before the first run, and the ones after, are
# in sdks/go/RELEASE.md. Bump Version in client.go first; a tag that exists is
# refused.
set -eu

remote=${1:?usage: release.sh <public git remote of withruntime.com/go>}
here=$(cd "$(dirname "$0")/.." && pwd)
top=$(git -C "$here" rev-parse --show-toplevel)
version=v$(sed -n 's/^const Version = "\(.*\)"/\1/p' "$here/client.go")
key="$HOME/.ssh/runtimeteam_signing.pub"

if [ -n "$(git -C "$top" status --porcelain -- sdks/go)" ]; then
  echo "sdks/go has uncommitted changes; release a commit." >&2
  exit 1
fi
[ -f "$key" ] || { echo "No runtimeteam signing key at $key." >&2; exit 1; }
(cd "$here" && gofmt -l . | grep . && { echo "gofmt would change the files above." >&2; exit 1; } || true)
(cd "$here" && go vet ./... && go test ./...)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
git clone -q "$remote" "$work/public"
cd "$work/public"
if git rev-parse -q --verify "refs/tags/$version" >/dev/null; then
  echo "$version is already released." >&2
  exit 1
fi
# The public tree is exactly sdks/go at HEAD, and nothing else.
git rm -rq --ignore-unmatch . >/dev/null
git -C "$top" archive HEAD:sdks/go | tar -x
git add -A
tree=$(git write-tree)
parent=$(git rev-parse -q --verify HEAD || true)
export GIT_AUTHOR_NAME=Runtime GIT_COMMITTER_NAME=Runtime
export GIT_AUTHOR_EMAIL=334032031+runtimeteam@users.noreply.github.com
export GIT_COMMITTER_EMAIL=$GIT_AUTHOR_EMAIL
commit=$(git -c gpg.format=ssh -c user.signingkey="$key" commit-tree -S "$tree" \
  ${parent:+-p "$parent"} -m "withruntime.com/go $version" </dev/null)
git push "$remote" "$commit:refs/heads/main" "$commit:refs/tags/$version"

# Ask the Go module proxy for the new version, so `go get` and pkg.go.dev see it now.
cd "$here"
GOPROXY=https://proxy.golang.org GOFLAGS=-mod=mod go list -m "withruntime.com/go@$version"
echo "Released withruntime.com/go $version as Runtime."
