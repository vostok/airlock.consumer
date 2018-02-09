#!/usr/bin/env bash
cd ..
curl https://github.com/skbkontur/cement/releases/download/v1.0.22/e6257f9699a456f4d1626424ab90d2cb27337188.zip -O -J -L
mkdir ./cement
unzip -o e6257f9699a456f4d1626424ab90d2cb27337188.zip -d ./cement
mono ../cement/dotnet/cm.exe init
curl https://raw.githubusercontent.com/vostok/cement-modules/master/settings -O -J -L
/bin/cp ./settings ~/.cement/
cd $OLDPWD
mono ../cement/dotnet/cm.exe update-deps
mono ../cement/dotnet/cm.exe build-deps -v
mono ../cement/dotnet/cm.exe build -v
dotnet publish -c Release -o out
