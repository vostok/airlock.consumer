FROM microsoft/dotnet:2.0-sdk-jessie AS build-env
WORKDIR /app

# copy csproj and restore as distinct layers
#COPY nuget.config ./
COPY . ./

# copy everything else and build
#COPY . ./
#RUN dotnet publish -c Release -o out

# build runtime image
FROM microsoft/dotnet:2.0-runtime-jessie

WORKDIR /app
COPY --from=build-env /app ./
COPY wait-for-it.sh /bin/wait-for-it.sh
RUN chmod +x /bin/wait-for-it.sh

ENTRYPOINT ["dotnet", "/app/Vostok.AirlockConsumer.Logs/out/Vostok.AirlockConsumer.Logs.dll"]
