#!/bin/bash
set -e

echo "Running devcontainer setup..."

# Update and install ant with JUnit support
sudo apt-get update
sudo apt-get install -y ant ant-optional

# Set Java version if jenv is available
if command -v jenv &> /dev/null; then
    jenv global 21 || echo "Warning: jdk21 is not installed"
fi

# Configure git remotes based on workspace owner
if [ -n "$WORKSPACE_OWNER_USERNAME" ]; then
    echo "Configuring git remotes for $WORKSPACE_OWNER_USERNAME workspace..."

    # Add user-specific remote if it doesn't exist
    if ! git remote | grep -q "^${WORKSPACE_OWNER_USERNAME}$"; then
        git remote add "$WORKSPACE_OWNER_USERNAME" "https://github.netflix.net/${WORKSPACE_OWNER_USERNAME}/cde-nfcassandra.git"
        echo "Added $WORKSPACE_OWNER_USERNAME remote"
    else
        echo "$WORKSPACE_OWNER_USERNAME remote already exists"
    fi

    # Set origin to no_push to prevent accidental pushes
    git remote set-url --push origin no_push
    echo "Set origin remote to no_push"
fi

# Generate IntelliJ IDEA files (ignore failures)
ant generate-idea-files || echo "Warning: generate-idea-files failed, continuing..."

echo "Devcontainer setup complete!"
