#!/usr/bin/env bash
cd ..
curl https://github.com/skbkontur/cement/releases/download/v1.0.28/19fce5bb72c28c461c2114310eceaa6ed85be457.zip -O -J -L
mkdir ./cement
unzip -o 19fce5bb72c28c461c2114310eceaa6ed85be457.zip -d ./cement
mono ../cement/dotnet/cm.exe reinstall
mono ../cement/dotnet/cm.exe init
curl https://raw.githubusercontent.com/vostok/cement-modules/master/settings -O -J -L
/bin/cp ./settings ~/.cement/
cd $OLDPWD
mono ../cement/dotnet/cm.exe update-deps
mono ../cement/dotnet/cm.exe build-deps -v
mono ../cement/dotnet/cm.exe build -v
dotnet publish -c Release -o out
