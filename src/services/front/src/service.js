import axios from "axios";


export const getFiles = async () =>
  await axios.get(process.env.REACT_APP_URI_HOST_SHOW_FILES, null, {});

export const getJoinedFileFromHDFS = async () =>
  await axios.get(process.env.REACT_APP_HDFS_HOST_GET_JOINED_FILES, null, {});

export const getPretransformedAFromHDFS = async () =>
  await axios.get(process.env.REACT_APP_HDFS_HOST_GET_PRETRANSFORMED_FILES, null, {});

export const uploadFiles = async (file) =>
  await axios.post(process.env.REACT_APP_UPLOAD_FILE, file, {
    headers: {  "Content-Type": "multipart/form-data" }
})

export const join = async (postRes) =>
  await axios.post(process.env.REACT_APP_START_ETL_JOIN, postRes);

