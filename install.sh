#!/bin/sh
set -e

BANNER='

     __   _     __
    / /_ (_)___/ /_ __
   / __// // _  /\ \ /
  / /_ / // /_/ / \ \
  \__//_/ \____/ /_\_\

'

echo "$BANNER"

REPO="tempoxyz/tidx"
BINARY="tidx"
VERSION="${1:-latest}"

detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux";;
        Darwin*) echo "darwin";;
        *)       echo "unsupported"; exit 1;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64)  echo "amd64";;
        amd64)   echo "amd64";;
        arm64)   echo "arm64";;
        aarch64) echo "arm64";;
        *)       echo "unsupported"; exit 1;;
    esac
}

OS=$(detect_os)
ARCH=$(detect_arch)
ASSET="${BINARY}-${OS}-${ARCH}"

echo "Detected: ${OS}/${ARCH}"
echo "Installing tidx ${VERSION}..."

if [ "$VERSION" = "latest" ]; then
    URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"
else
    URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}"
fi

INSTALL_DIR="${HOME}/.local/bin"
mkdir -p "$INSTALL_DIR"

echo "Downloading from ${URL}..."
curl -fsSL "$URL" -o "${INSTALL_DIR}/${BINARY}"
chmod +x "${INSTALL_DIR}/${BINARY}"

echo ""
echo "Installed ${BINARY} to ${INSTALL_DIR}/${BINARY}"

if ! echo "$PATH" | grep -q "${INSTALL_DIR}"; then
    SHELL_NAME=$(basename "$SHELL")
    case "$SHELL_NAME" in
        zsh)
            RC_FILE="${ZDOTDIR:-$HOME}/.zshenv"
            PATH_EXPORT='export PATH="$HOME/.local/bin:$PATH"'
            ;;
        bash)
            if [ -f "$HOME/.bash_profile" ]; then
                RC_FILE="$HOME/.bash_profile"
            else
                RC_FILE="$HOME/.bashrc"
            fi
            PATH_EXPORT='export PATH="$HOME/.local/bin:$PATH"'
            ;;
        fish)
            RC_FILE="$HOME/.config/fish/config.fish"
            PATH_EXPORT='fish_add_path $HOME/.local/bin'
            ;;
        sh|dash)
            RC_FILE="$HOME/.profile"
            PATH_EXPORT='export PATH="$HOME/.local/bin:$PATH"'
            ;;
        *)
            RC_FILE=""
            PATH_EXPORT='export PATH="$HOME/.local/bin:$PATH"'
            ;;
    esac

    if [ -n "$RC_FILE" ]; then
        if ! grep -q '.local/bin' "$RC_FILE" 2>/dev/null; then
            mkdir -p "$(dirname "$RC_FILE")"
            echo "" >> "$RC_FILE"
            echo "# Added by tidx installer" >> "$RC_FILE"
            echo "$PATH_EXPORT" >> "$RC_FILE"
            echo "Added ~/.local/bin to PATH in $RC_FILE"
            echo "Run 'source $RC_FILE' or restart your shell to use tidx"
        fi
    else
        echo ""
        echo "Add ~/.local/bin to your PATH:"
        echo "  $PATH_EXPORT"
    fi
fi

echo ""
echo "Get started:"
echo "  tidx init      # Initialize config.toml"
echo "  tidx up        # Start indexing"
echo "  tidx status    # View sync status"
echo "  tidx --help    # See all commands"
