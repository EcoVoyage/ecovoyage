#!/bin/bash
echo "Installing ecovoyage in development mode..."
cd /workspace && python -m pip install -e . --config-settings editable_mode=compat --use-pep517
echo "Development environment activated with editable install" 