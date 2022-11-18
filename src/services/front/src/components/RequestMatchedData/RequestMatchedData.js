import React, { useState, useEffect } from 'react'
import {getJoinedFileFromHDFS} from '../../service';
import ToolbarWrapper from '../Toolbar/Toolbar';
import styles from "./requestMatchedData.module.css";
import axios from 'axios';
import Snackbar from '@mui/material/Snackbar';
import MuiAlert from '@mui/material/Alert';

const Alert = React.forwardRef(function Alert(props, ref) {
  return <MuiAlert elevation={6} ref={ref} variant="filled" {...props} />;
});

export default function RequestMatchedData({matchedFiles, setMatchedFiles}) {

    const [results, setResults] = useState(null);
    const [open, setOpen] = useState(false);
    const [loadingJoin, setLoadingJoin] = useState(false);
    const NAME_OF_OTHER_CLUSTER = process.env.REACT_APP_OTHER_CLUSTER;
    const FileDownload = require('js-file-download');
    const [files, setFiles] = useState(null)

    const filesRes = [
            {"file_name": NAME_OF_OTHER_CLUSTER + "_matched_files_1.csv"},
            {"file_name": NAME_OF_OTHER_CLUSTER + "_matched_files_2.csv"},
        ]


    useEffect(() => {
        fetchFiles();
    }, []);

    const fetchFiles = async () => {
        try{
            await axios.all([getJoinedFileFromHDFS()]).then(
                axios.spread((...allData) => {
                    setFiles(allData)
                    console.log('Joined FIles: ', files);

                })
            )
        }catch(error){
            setFiles(filesRes)
            console.log('ERROR ', error);
        }

    }
    useEffect(() => {
        if (!files) return;

        let joined_files = files[0].data
        let matched_data_array = [];


        if (joined_files.documents.length > 0)
            for(let el of joined_files.documents){
                let obj = {}
                obj.name = el.name;
                matched_data_array.push(obj);
            }

        setMatchedFiles(matched_data_array)

    }, [files]);

    const handleClose = (event, reason) => {
        if (reason === 'clickaway') {
        return;
        }

        setOpen(false);
    };

    const handleClickRequestData = async (directory, project_name ) => {
        const data = {
            "project_name": project_name,
        }
        try{
            const response = await axios.get(process.env.REACT_APP_OTHER_CLUSTER_URL_SEND_DATA, { params: { data } });
            FileDownload(response.data, 'download.csv')
            setResults(response);
        }catch(error){
            console.log('ERROR ', error);
        }
    };

    const displayMatchedData = matchedFiles?.map((item, idx) => {
        return (
            <div className={styles.FileWrapper} key={idx}>
                <div className={styles.FileItem} >
                    <p>{item.name}</p>
                    <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                         handleClickRequestData("matched_data", item.name)}}>Request Data</button>
                    </div>
                </div>
            </div>
        )
    })

  return (
    <ToolbarWrapper>
        {loadingJoin? <h2> Request matched data from </h2> : <> {!results ? <div>
            <div>
                <p className={styles.MarginTop}> Results </p>
                {displayMatchedData}
            </div>
            <Snackbar open={open} autoHideDuration={4000} onClose={handleClose}>
            <Alert onClose={handleClose} severity="error" sx={{width: '100%'}}>
            You must selected one file of each category and fill the prediction size and noise!
            </Alert>
            </Snackbar>
            </div>:
            <div>
            </div>
        }</>}

    </ToolbarWrapper>
  )
}
