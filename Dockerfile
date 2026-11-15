ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE}

ENV TZ=Asia/Shanghai
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 1. System dependencies + Supercronic
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tzdata \
    procps \
    util-linux \
    git \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ARG SUPERCRONIC_VERSION=v0.2.29
RUN SUPERCRONIC="supercronic-linux-${TARGETARCH}" && \
    curl -fsSLO "https://ghproxy.net/https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/${SUPERCRONIC}" && \
    chmod +x "${SUPERCRONIC}" && \
    mv "${SUPERCRONIC}" /usr/local/bin/supercronic

WORKDIR /app

# 2. Python dependencies (cached layer)
COPY requirements-portfolio.txt .
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip config set global.extra-index-url https://pypi.org/simple && \
    pip install --no-cache-dir -r requirements-portfolio.txt

# 3. Application code
COPY . .

# 3b. Install data-access-lib from the git submodule.
COPY data-access-lib /app/data-access-lib
RUN pip install --no-build-isolation --no-deps /app/data-access-lib

# 4. Permissions
RUN chmod +x /app/start.portfolio.sh

# 5. Entrypoint
CMD ["/app/start.portfolio.sh"]
