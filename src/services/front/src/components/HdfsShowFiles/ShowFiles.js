import React, { useEffect, useState } from 'react';
import ToolbarWrapper from '../Toolbar/Toolbar';
import styles from './showFiles.module.css';
import Snackbar from '@mui/material/Snackbar';
import MuiAlert from '@mui/material/Alert';
import {getJoinedFileFromHDFS, getPretransformedAFromHDFS} from '../../service';
import axios from "axios";
import fileDownload from 'js-file-download'

const Alert = React.forwardRef(function Alert(props, ref) {
  return <MuiAlert elevation={6} ref={ref} variant="filled" {...props} />;
});

export default function HdfsShowFiles({joinedFiles, setJoinedFiles, matchedFiles, setMatchedFiles, transformedFiles, setTransformedFiles}) {

    const [properties, setProperties] = useState({});
    const [loadingJoin, setLoadingJoin] = useState(false);
    const [results, setResults] = useState(null);
    const [open, setOpen] = useState(false);
    const [files, setFiles] = useState(null)
    const NAME_OF_CLUSTER = process.env.REACT_APP_NAME_OF_CLUSTER;

    const filesRes = [
    {
        "data": {
            "files": [
                {
                    "columns": [
                        "NCID",
                        "last_name",
                        "first_name",
                        "midl_name",
                        "street_name",
                        "res_city_desc",
                        "birth_age"
                    ],
                    "name": "joined_files_1.csv"
                },
                {
                    "columns": [
                        "NCID",
                        "last_name",
                        "first_name",
                        "midl_name",
                        "street_name",
                        "res_city_desc",
                        "birth_age"
                    ],
                    "name": "joined_files_2.csv"
                },
            ]
        }
    },
    {
        "data": {
            "files": [
                {
                    "columns": [
                        "NCID",
                        "last_name",
                        "first_name",
                        "midl_name",
                        "street_name",
                        "res_city_desc",
                        "birth_age"
                    ],
                    "name": "matched_file_1.csv"
                },
                {
                    "columns": [
                        "NCID",
                        "last_name",
                        "first_name",
                        "midl_name",
                        "street_name",
                        "res_city_desc",
                        "birth_age"
                    ],
                    "name": "matched_file_2.csv"
                },
            ]
        }
    },
    {
        "data": {
            "files": [
                {
                    "columns": [
                        "NCID",
                        "last_name",
                        "first_name",
                        "midl_name",
                        "street_name",
                        "res_city_desc",
                        "birth_age"
                    ],
                    "name": "transformed_file_1.csv"
                },
                {
                    "columns": [
                        "NCID",
                        "last_name",
                        "first_name",
                        "midl_name",
                        "street_name",
                        "res_city_desc",
                        "birth_age"
                    ],
                    "name": "transformed_file_2.csv"
                },
            ]
        }
    }
]
    useEffect(() => {
        fetchFiles();
    }, []);

    const fetchFiles = async () => {
        try{
            await axios.all([getJoinedFileFromHDFS(),
                             getPretransformedAFromHDFS()]).then(
                axios.spread((...allData) => {
                    setFiles(allData)
                    console.log('DATA from APi ', allData);

                })
            )
        }catch(error){
            setFiles(filesRes)
            console.log('ERROR ', error);
        }

    }
    useEffect(() => {
        if (!files) return;

        let joined_data_array = [];
        let transformed_data_array = [];

        if (files[0].data.documents.length !== 0)
            for(let el of files[0].data.documents){
                let obj = {}
                obj.name = el.name;
                joined_data_array.push(obj);
            }

        if (files[1].data.documents.length !== 0)
            for(let el of files[1].data.documents){
                let obj = {}
                obj.name = el.name;
                transformed_data_array.push(obj);
            }

        setJoinedFiles(joined_data_array)
        setTransformedFiles(transformed_data_array)

    }, [files]);


    const handleClose = (event, reason) => {
        if (reason === 'clickaway') {
        return;
        }

        setOpen(false);
    };

    const handleClickDownload = async (directory, file_name) => {
        const params = {
            file: file_name
        };
        try{
            const response = await axios.get(process.env.REACT_APP_HDFS_HOST_DOWNLOAD_FILE + directory, {params}).then((response) => {
            fileDownload(response.data, 'download.csv')
        })
            setResults(response);
        }catch(error){
            console.log('ERROR ', error);
        }
    };

    const handleClickDelete = async (directory, file_name) => {
        const params = {
            file: file_name
        };
        try{
            const response = await axios.delete(process.env.REACT_APP_HDFS_HOST_DOWNLOAD_FILE + directory, {params}).then((response) => {
        })
            setResults(response);
            console.log(file_name + ' has been deleted');

            var index = 0
            var files

            if (directory === 'joined_data') {
                files = setJoinedFiles

            }
            else {
                files = setTransformedFiles
            }

            console.log(joinedFiles + 'asdas');

            files((current) =>
                current.filter((projects) => projects.name !== file_name)
            );

        }catch(error){
            console.log('ERROR ', error);
        }
    };

    const displayJoinedFiles = joinedFiles?.map((item, idx) => {
    return (
        <div className={styles.FileWrapper} key={idx}>
            <div className={styles.FileItem} >
                <p>{item.name}</p>
                <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDownload("joined_data", item.name.toString() )}}>Download</button>
                </div>
                <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDelete("joined_data", item.name.toString() )}}>Delete</button>
                </div>
            </div>
        </div>
        )
    })

    const displayTransformedData = transformedFiles?.map((item, idx) => {
    return (
        <div className={styles.FileWrapper} key={idx}>
            <div className={styles.FileItem} >
                <p>{item.name}</p>
                <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDownload("pretransformed_data", item.name.toString() )}}>Download</button>
                </div>
                <div>
                    <button className={styles.MarginTopXSmall} onClick={() => {
                        handleClickDelete("pretransformed_data", item.name.toString() )}}>Delete</button>
                </div>
            </div>
        </div>
        )
    })

    return (
        <ToolbarWrapper>
            {loadingJoin? <h2>LOADING RESULTS</h2> : <> {!results ? <div>
                <div>
                    <p className={styles.MarginBottom}>Joined Data</p>
                    {displayJoinedFiles}
                </div>
                <br></br>
                <div>
                    <p className={styles.MarginBottom}>Transformed {NAME_OF_CLUSTER} Data</p>
                    {displayTransformedData}
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
