FROM node:20

WORKDIR /app

# Install Angular CLI globally
RUN npm install -g @angular/cli@17.3.6

# Copy package.json and package-lock.json
COPY package/package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application
COPY package/ ./

# Expose port 4200
EXPOSE 4200

# Start the application, binding to 0.0.0.0 so it can be accessed outside the container
CMD ["npm", "start", "--", "--host", "0.0.0.0", "--poll", "2000"]
