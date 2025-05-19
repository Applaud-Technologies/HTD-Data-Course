# Setting Up Docker for SQL Server

## Introduction

Database environment setup traditionally creates friction in development pipelines, with inconsistent configurations leading to the classic "works on my machine" problem. For financial data engineering projects, where database reliability is paramount, these inconsistencies can introduce costly bugs and deployment delays. 

Docker containerization elegantly addresses this challenge by encapsulating SQL Server with all its dependencies in isolated, portable packages. This containerized approach enables teams to maintain consistent database environments across development, testing, and production while eliminating complex installation procedures. Throughout this lesson, you'll learn to implement a Docker-based SQL Server environment that streamlines your database workflow and aligns with modern DevOps practices.

## Prerequisites

From our previous lesson on relational databases, recall that SQL Server is a robust relational database management system that implements the principles we discussed: tables organized into rows and columns, primary and foreign keys for establishing relationships, and ACID-compliant transactions. The containerized approach we'll explore today doesn't change these fundamental database concepts but rather simplifies how we deploy and manage the database engine itself. Understanding tables, relationships, and basic SQL Server architecture will help contextualize our containerization work.

## Learning Outcomes

By the end of this lesson, you will be able to:

1. Implement a containerized SQL Server environment using Docker

## Implement a containerized SQL Server environment using Docker

### Installing Docker Desktop

Before we can run containerized SQL Server, we need to install Docker Desktop, which provides the container runtime environment. Docker Desktop works similarly to virtual machines but with significantly less overhead, sharing the host operating system's kernel while maintaining isolation between containers.

```bash
# Download Docker Desktop from https://www.docker.com/products/docker-desktop/

# After installation, verify Docker is working by checking the version
docker --version
```

```console
Docker version 24.0.6, build ed223bc
```

```bash
# Configure Docker Desktop resources (via GUI)
# 1. Open Docker Desktop
# 2. Click on Settings (gear icon)
# 3. Go to Resources section
# 4. Allocate at least:
#    - 2 CPUs
#    - 4GB RAM
#    - 1GB Swap
#    - 60GB Disk Image size

# Verify virtualization is enabled
# For Windows (PowerShell):
systeminfo | findstr "Virtualization"
```

```console
Virtualization Enabled In Firmware: Yes
```

Docker Desktop installation represents a key shift in the development paradigm - instead of installing database engines directly on your machine, you're setting up a containerization platform that allows multiple isolated environments to coexist, much like how microservices architecture isolates application components.

### Docker images

Docker images function conceptually like class definitions in object-oriented programming - they're read-only templates that define everything needed to run an application, including code, runtime, libraries, and environment variables. The SQL Server Docker image from Microsoft contains a pre-configured SQL Server installation ready to be instantiated as a running container.

```bash
# Search for Microsoft SQL Server images
docker search mssql

# Pull the official Microsoft SQL Server 2022 image
docker pull mcr.microsoft.com/mssql/server:2022-latest

# Verify the image was downloaded
docker images
```

```console
REPOSITORY                      TAG           IMAGE ID       CREATED        SIZE
mcr.microsoft.com/mssql/server  2022-latest   b33a9e28f5e7   2 months ago   1.58GB
```

```bash
# View the layers that make up the SQL Server image
docker history mcr.microsoft.com/mssql/server:2022-latest
```

```console
IMAGE          CREATED        CREATED BY                                      SIZE      COMMENT
b33a9e28f5e7   2 months ago   /bin/sh -c #(nop)  CMD ["/opt/mssql/bin/sql…   0B        
<missing>      2 months ago   /bin/sh -c #(nop)  ENTRYPOINT ["/opt/mssql/…   0B        
<missing>      2 months ago   /bin/sh -c #(nop) COPY file:40a0b13b45a6f35…   154B      
...
```

The concept of Docker images mirrors artifact repositories in software development - they provide versioned, immutable packages that can be deployed consistently. Just as you might publish built application artifacts to Maven or npm repositories, Microsoft publishes verified SQL Server images to Docker Hub and Microsoft Container Registry.

> **Note:** Make sure to choose the appropriate SQL Server edition for your needs. The Developer edition is free for non-production use and contains all Enterprise edition features, making it ideal for development environments.

### Container lifecycle

Containers are the runtime instances of Docker images, comparable to objects instantiated from classes. The container lifecycle mirrors application lifecycle management patterns, with clear states and transitions that developers can control.

```bash
# Create and start a SQL Server container
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 1433:1433 \
           --name sql_server_container \
           --restart unless-stopped \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Check running containers
docker ps
```

```console
CONTAINER ID   IMAGE                                      COMMAND                  CREATED         STATUS         PORTS                    NAMES
3a7b12c65a2f   mcr.microsoft.com/mssql/server:2022-latest "/opt/mssql/bin/perm…"  10 seconds ago  Up 8 seconds   0.0.0.0:1433->1433/tcp   sql_server_container
```

```bash
# Stop the container
docker stop sql_server_container

# Start a stopped container
docker start sql_server_container

# Remove a container (must be stopped first)
docker stop sql_server_container
docker rm sql_server_container

# Run in interactive mode (see logs in real-time)
# Press Ctrl+C to exit but keep container running
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 1433:1433 \
           --name sql_server_interactive \
           mcr.microsoft.com/mssql/server:2022-latest
```

Managing container lifecycle reflects infrastructure-as-code principles - operations traditionally performed manually through GUI tools become scriptable, repeatable commands. This approach aligns with DevOps practices by enabling automated environment provisioning and consistent configuration management.

### Environment configuration

Configuration management is critical in both database administration and software development. Docker allows us to configure SQL Server through environment variables, which parallels how modern applications use environment-based configuration for different deployment contexts.

```bash
# Basic SQL Server container with required environment variables
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 1433:1433 \
           --name sql_server_basic \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Advanced configuration with optional parameters
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -e "MSSQL_PID=Developer" \
           -e "MSSQL_MEMORY_LIMIT_MB=4096" \
           -e "MSSQL_COLLATION=SQL_Latin1_General_CP1_CI_AS" \
           -p 1433:1433 \
           --name sql_server_advanced \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Using an environment file for better security
# First, create a .env file:
echo "ACCEPT_EULA=Y
SA_PASSWORD=YourStrong@Passw0rd
MSSQL_PID=Developer" > sql_server.env

# Then use the env file in your docker command
docker run --env-file sql_server.env \
           -p 1433:1433 \
           --name sql_server_env_file \
           -d mcr.microsoft.com/mssql/server:2022-latest
```

This environment-based configuration mirrors the twelve-factor app methodology widely adopted in cloud-native development. Instead of complex configuration files, simple environment variables control application behavior, making containers easily portable across different environments.

> **Security Warning:** Never hardcode the SA password in production scripts or source control. For production deployments, use Docker secrets, environment files, or orchestration platform secrets management.

### Port mapping

Container networking concepts share similarities with API gateway patterns in microservices architecture. Port mapping creates a network bridge between the host machine and the containerized application, allowing communication while maintaining isolation.

```bash
# Standard port mapping (host port 1433 to container port 1433)
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 1433:1433 \
           --name sql_server_standard_port \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Using a different host port (14330) to avoid conflicts
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 14330:1433 \
           --name sql_server_dev \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Check port mappings for a container
docker port sql_server_dev
```

```console
1433/tcp -> 0.0.0.0:14330
```

```bash
# Run multiple SQL Server instances with different port mappings
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 14331:1433 \
           --name sql_server_test \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Connection string would use localhost,14330 for dev and localhost,14331 for test
```

Port mapping demonstrates an important principle in both containerization and API design: while internals maintain consistent addressing (SQL Server always listens on port 1433 inside the container), external interfaces can be adapted to fit the broader system needs (mapping to different host ports).

**Diagram Description:** Network diagram showing how the host machine, container, and port mapping work together, with arrows indicating traffic flow from client applications through mapped ports to the containerized SQL Server.

### Volume persistence

Data persistence is a critical concern for database systems. Docker volumes solve this by providing persistent storage that survives container lifecycle events, similar to how external storage services decouple data from application instances in cloud architectures.

```bash
# Create a named volume for SQL Server data
docker volume create sql_server_data

# Run SQL Server with the named volume mounted
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 1433:1433 \
           -v sql_server_data:/var/opt/mssql \
           --name sql_server_persistent \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Inspect the volume to see its details
docker volume inspect sql_server_data
```

```console
[
    {
        "CreatedAt": "2023-10-15T14:30:22Z",
        "Driver": "local",
        "Labels": {},
        "Mountpoint": "/var/lib/docker/volumes/sql_server_data/_data",
        "Name": "sql_server_data",
        "Options": {},
        "Scope": "local"
    }
]
```

```bash
# Using host directory mounting (Windows example)
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 1433:1433 \
           -v C:/SqlServerData:/var/opt/mssql \
           --name sql_server_host_mount \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Using host directory mounting (Mac/Linux example)
docker run -e "ACCEPT_EULA=Y" \
           -e "SA_PASSWORD=YourStrong@Passw0rd" \
           -p 1433:1433 \
           -v /Users/username/SqlServerData:/var/opt/mssql \
           --name sql_server_host_mount \
           -d mcr.microsoft.com/mssql/server:2022-latest
``````

The separation of data (volumes) from application logic (containers) reflects clean architecture principles in software development, where proper separation of concerns enhances system maintainability and flexibility. Just as domain logic should be independent of data storage mechanisms, containerized applications should maintain separation from their persistent data.

### Verifying SQL Server operation

Once your SQL Server container is running, verification ensures it's operational before connecting client applications. This step is analogous to smoke testing in the deployment pipeline.

```bash
# Check container logs to verify SQL Server started successfully
docker logs sql_server_container
```

```console
2023-10-15 14:35:22.42 Server      Microsoft SQL Server 2022 (RTM-CU8) (KB5029666) - 16.0.4085.2 (X64) 
    Jul 25 2023 14:02:40 
    Copyright (C) 2022 Microsoft Corporation
    Developer Edition (64-bit) on Linux (Ubuntu 20.04.6 LTS) <X64>
2023-10-15 14:35:22.42 Server      UTC adjustment: 0:00
2023-10-15 14:35:22.42 Server      (c) Microsoft Corporation.
2023-10-15 14:35:22.42 Server      All rights reserved.
2023-10-15 14:35:22.42 Server      Server process ID is 40.
2023-10-15 14:35:22.42 Server      Logging SQL Server messages in file '/var/opt/mssql/log/errorlog'.
2023-10-15 14:35:22.43 Server      Registry startup parameters: 
2023-10-15 14:35:22.43 Server          -d /var/opt/mssql/data/master.mdf
2023-10-15 14:35:22.43 Server          -l /var/opt/mssql/data/mastlog.ldf
2023-10-15 14:35:22.43 Server          -e /var/opt/mssql/log/errorlog
...
2023-10-15 14:35:23.82 Server      SQL Server is now ready for client connections. This is an informational message; no user action is required.
```

```bash
# Execute a command inside the container to verify SQL Server is responding
docker exec -it sql_server_container \
    /opt/mssql-tools/bin/sqlcmd \
    -S localhost \
    -U sa \
    -P "YourStrong@Passw0rd" \
    -Q "SELECT @@VERSION"
```

```console
Microsoft SQL Server 2022 (RTM-CU8) (KB5029666) - 16.0.4085.2 (X64) 
    Jul 25 2023 14:02:40 
    Copyright (C) 2022 Microsoft Corporation
    Developer Edition (64-bit) on Linux (Ubuntu 20.04.6 LTS) <X64>
```

```bash
# Create a test database to verify full functionality
docker exec -it sql_server_container \
    /opt/mssql-tools/bin/sqlcmd \
    -S localhost \
    -U sa \
    -P "YourStrong@Passw0rd" \
    -Q "CREATE DATABASE TestDB; SELECT Name FROM sys.Databases;"
```

```console
Name
--------------------------------------------------------------------------------------------------------------------------------
master
tempdb
model
msdb
TestDB
```

This verification step mirrors integration testing principles in software development - before declaring a deployment successful, you confirm the system actually responds correctly to basic operations.
## Coming Up

In the next lesson, "Getting Started with Azure Data Studio," you'll learn how to use Azure Data Studio to connect to your containerized SQL Server instance. Topics will include navigating the Azure Data Studio interface, establishing database connections, and running your first SQL queries. This will allow you to interact with the SQL Server instance you've just containerized.

## Conclusion

In this lesson, we explored how Docker transforms SQL Server deployment from a complex installation process into a series of straightforward commands. You've learned to create, configure, and manage containerized SQL Server instances that provide consistent, isolated database environments. 

This approach mirrors fundamental software engineering principles: infrastructure as code, environment isolation, and separation of concerns between application and data layers. The containerization techniques you've practiced not only save development time but establish a foundation for more sophisticated deployment scenarios, including multi-container systems and orchestrated environments. As you move to the next lesson on Azure Data Studio, you'll build on this containerized foundation by connecting to your SQL Server instance and executing queries through a powerful, purpose-built interface.

## Glossary

**Container**: A lightweight, standalone executable package that contains an application and all its dependencies, running in isolation.

**Docker**: An open-source platform that automates the deployment, scaling, and management of applications using containerization.

**Image**: A read-only template containing application code, libraries, dependencies, and configuration used to create containers.

**Port mapping**: The redirection of traffic from a port on the host machine to a port in the container, enabling external access to containerized services.

**Volume**: A persistent storage mechanism in Docker that allows data to exist independently of container lifecycle.

**EULA**: End User License Agreement, which must be accepted when using Microsoft SQL Server.

**SA account**: System Administrator account in SQL Server, the default administrative user.

**Container lifecycle**: The different states a container can exist in (created, running, paused, stopped, removed) and the transitions between them.

**Docker Desktop**: A Docker application for Windows and macOS that provides the Docker Engine, CLI client, and additional tools.

**Environment variable**: A key-value pair that configures the containerized application's behavior, set using the -e flag in Docker commands.