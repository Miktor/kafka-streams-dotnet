FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["Streams/Streams.csproj", "Streams/"]
RUN dotnet restore "Streams/Streams.csproj"
COPY . .
WORKDIR "/src/Streams"
RUN dotnet build "Streams.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Streams.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Streams.dll"]
