FROM microsoft/dotnet:2.0-sdk AS build-env
WORKDIR /app

# copy csproj and restore as distinct layers
COPY nuget.config ./
COPY . ./
RUN dotnet restore

# copy everything else and build
COPY . ./
RUN dotnet publish -c Release -o out

# build runtime image
FROM microsoft/dotnet:2.0-runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build-env /app ./
ENTRYPOINT ["dotnet", "/app/Vostok.AirlockConsumer.Logs/out/Vostok.AirlockConsumer.Logs.dll"]