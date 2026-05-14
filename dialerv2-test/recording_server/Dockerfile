FROM node:20-slim

WORKDIR /app

# Install dependencies first for better caching
COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi \
  && npm cache clean --force

# Copy source
COPY server.js ./

# Default recordings directory
ENV SAVE_DIR=/opt/jambonz-recorder/recordings \
    PORT=8080

# Ensure recordings directory exists and mountable
RUN mkdir -p /opt/jambonz-recorder/recordings
VOLUME ["/opt/jambonz-recorder/recordings"]

EXPOSE 8080

CMD ["npm", "start"]
