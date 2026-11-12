FROM mcr.microsoft.com/dotnet/sdk:10.0
RUN apt-get update && apt-get install -y --no-install-recommends unixodbc ca-certificates curl unzip procps \
 && echo "deb [trusted=yes] https://public.dhe.ibm.com/software/ibmi/products/odbc/debs 1.1.0 main" > /etc/apt/sources.list.d/ibmi-acs.list \
 && apt-get update && apt-get install -y ibm-iaccess && rm -rf /var/lib/apt/lists/* \
 && curl -sSL https://aka.ms/getvsdbgsh | bash /dev/stdin -v latest -l /root/vsdbg
