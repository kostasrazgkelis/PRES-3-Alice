FROM node:alpine

# add arguments #
ARG REACT_APP_UPLOAD_FILE
ARG REACT_APP_START_ETL_JOIN
ARG REACT_APP_URI_HOST_SHOW_FILES
ARG REACT_APP_URI_HOST_TAKE_DATA
ARG REACT_APP_OTHER_CLUSTER_URL
ARG REACT_APP_HDFS_HOST_SHOW_FILE
ARG REACT_APP_HDFS_HOST_DOWNLOAD_FILE
ARG REACT_APP_HDFS_HOST_GET_PRETRANSFORMED_FILES
ARG REACT_APP_HDFS_HOST_GET_JOINED_FILES
ARG REACT_APP_OTHER_CLUSTER_URL_SEND_DATA

ENV REACT_APP_UPLOAD_FILE=$REACT_APP_UPLOAD_FILE
ENV REACT_APP_START_ETL_JOIN=$REACT_APP_START_ETL_JOIN
ENV REACT_APP_URI_HOST_SHOW_FILES=$REACT_APP_URI_HOST_SHOW_FILES
ENV REACT_APP_URI_HOST_TAKE_DATA=$REACT_APP_URI_HOST_TAKE_DATA
ENV REACT_APP_OTHER_CLUSTER_URL=$REACT_APP_OTHER_CLUSTER_URL
ENV REACT_APP_HDFS_HOST_SHOW_FILE=$REACT_APP_HDFS_HOST_SHOW_FILE
ENV REACT_APP_HDFS_HOST_DOWNLOAD_FILE=$REACT_APP_HDFS_HOST_DOWNLOAD_FILE
ENV REACT_APP_HDFS_HOST_GET_PRETRANSFORMED_FILES=$REACT_APP_HDFS_HOST_GET_PRETRANSFORMED_FILES
ENV REACT_APP_HDFS_HOST_GET_JOINED_FILES=$REACT_APP_HDFS_HOST_GET_JOINED_FILES
ENV REACT_APP_OTHER_CLUSTER_URL_SEND_DATA=$REACT_APP_OTHER_CLUSTER_URL_SEND_DATA
# stop argumetns #

WORKDIR /react-front

COPY ./infrastructures/front /react-front

RUN npm install
RUN npm install js-file-download --save
RUN npm install --save react-helmet

COPY ./src/services/front /react-front

RUN npm run build
CMD ["npm", "start"]