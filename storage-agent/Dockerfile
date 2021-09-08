FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

ENV PORT=8080
RUN mkdir /storage-agent
COPY . /storage-agent
WORKDIR /storage-agent
RUN microdnf install nodejs python3 make gcc gcc-c++
RUN npm install
EXPOSE 8080
CMD [ "node" , "storage-agent.js"]
