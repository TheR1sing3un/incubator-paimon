#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Installing dependencies..."
npm install

echo "Building..."
npm run build

echo "Copying dist to project root..."
rm -rf ../dist
cp -r dist ../dist

echo "Build output: $(cd .. && pwd)/dist"
