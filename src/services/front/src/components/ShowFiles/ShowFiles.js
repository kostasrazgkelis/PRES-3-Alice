import React, { useEffect, useState } from 'react';
import ToolbarWrapper from '../Toolbar/Toolbar';
import styles from './showFiles.module.css';
import Snackbar from '@mui/material/Snackbar';
import MuiAlert from '@mui/material/Alert';
import { getFiles } from '../../service';
import axios from 'axios';


const Alert = React.forwardRef(function Alert(props, ref) {
  return <MuiAlert elevation={6} ref={ref} variant="filled" {...props} />;
});

export default function ShowFiles({filesA, setFilesA}) {

    const [properties, setProperties] = useState({});
    const [loadingJoin, setLoadingJoin] = useState(false);
    const [results, setResults] = useState(null);
    const [open, setOpen] = useState(false);
    const [files, setFiles] = useState(null);
    const NAME_OF_CLUSTER = process.env.REACT_APP_NAME_OF_CLUSTER;

    const filesRes = {
        "files_a": [
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
                "name": "100K_A.csv"
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
                "name": "50K_A.csv"
            }
        ],
        "files_b": [
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
                "name": "100K_B.csv"
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
                "name": "50K_B.csv"
            }
        ]
    }

    useEffect(() => {
        fetchFiles();
    }, []);

    const fetchFiles = async() => {
           try{
            let res = await getFiles();
            console.log('Response ', res);
            setFiles(res.data);

        }catch(error){
            setFiles(filesRes)
            console.log('ERROR ', error);
        }
      
    }
    useEffect(() => {
        if(!files) return;

        let a = files['files'];
        let FAarr = [];
        /* Manipulation of A files */
        for(let el of a){
            let obj = {}
            let columns = [];
            obj.name = el.name;
            obj.selected = false;
            for(let col of el.columns){
                columns.push({
                    name: col,
                    selected: true
                })
            }
            obj.columns = columns;
            FAarr.push(obj);
        }
        setFilesA(FAarr)
      
        console.log('FINAL FILES ', filesA);
    }, [files]);

    const handeProperties = (event) =>{
        const { name, value } = event.target;
        console.log('HANDLE CHANGES ', name, value);
        var obj = {...properties, [name]: value}
        setProperties(obj)
    }

    const onColumnSelected = (event) =>{
        const { name, checked } = event.target;
        console.log('ON COLUMN SELECTED ', event.target.name, event.target.checked);
        /* Split the name to seperate file name from file's column */
        const nameArr = name.split("$");
        
        let newArr = [];
        
        newArr = [...filesA];
        const newCols = newArr[nameArr[0]].columns.map(col => {
           /*  console.log('CONSOLE OBJ', col); */
            if(col.name === nameArr[1]){
                console.log('col name ', col.name, col.selected)
                 return {...col, selected: !col.selected};
            }
            return col;
        });
        newArr[nameArr[0]] = {...newArr[nameArr[0]], columns: newCols};
        setFilesA(newArr);
        
    }

    const onFileSelectedA = (event) => {
        const { name, checked } = event.target;

        const newArr = filesA.map((obj) => {
            if (obj.name === name) {
                return {...obj, selected: checked};
            }
            else{
                return {...obj, selected: false};
            }
        });
        setFilesA(newArr);
    }

    const join = async() => {
        let postRes = {}
        let obj = {};

        for(let el of filesA){
            if(el.selected === true){
                obj.name = el.name;
                let cols = [];
                for(var col of el.columns){
                    if(col.selected) cols.push(col.name);
                }
                 obj.columns = cols;
            }
        }

        if(!obj || !properties.noise || !properties.matching_field){
            setOpen(true);
            return;
        }

        postRes.file = obj;
        postRes.noise = properties.noise;
        postRes.matching_field = properties.matching_field;

        /* POST HERE */
        console.log('Post Res', postRes);
        setLoadingJoin(true);
        try{
            /* let res = await join(postRes);
            console.log('Response ', res); */
            const response = await axios.post(process.env.REACT_APP_URI_HOST_TAKE_DATA, postRes);
            console.log('RESPONSE ', response);
            setResults(response.data);
            setLoadingJoin(false);

        }catch(error){
            console.log('ERROR ', error);
            setLoadingJoin(false);
        }
    }

    const handleClose = (event, reason) => {
        if (reason === 'clickaway') {
        return;
        }

        setOpen(false);
    };

    const displayAFiles = filesA?.map((item, idx) => {
        return (
            <div className={styles.FileWrapper} key={idx}>
                <div className={styles.FileItem} >
                    <p>{item.name}</p>
                    <div className={styles.Columns}>
                        {item.columns.map((column, index) => { return (
                            <div key={index}>
                                <p className='pLight'>{column.name}</p>
                                <input type="checkbox" name={`${idx}$${column.name}$A`} checked={column.selected} onChange={onColumnSelected}/>
                            </div>
                        )})}
                    </div>
                </div>
                <input type="checkbox" name={`${item.name}`} checked={item.selected} onChange={onFileSelectedA}/>
            </div>
        )
    })

    return (
        <ToolbarWrapper>
            {loadingJoin? <h2>LOADING RESULTS</h2> : <> {!results ? <div>
                <div>
                <p className={styles.MarginBottom}> {NAME_OF_CLUSTER} </p>
                    {displayAFiles}
                </div>

                <div className={styles.MarginTopSmall}>
                <input placeholder='Add noise' name='noise' type='number' onChange={handeProperties}/>
                <input placeholder='Add matching field' name='matching_field' type='text' onChange={handeProperties}/>

                </div>

                <button className={styles.MarginTopXSmall} onClick={join}>Transform Files</button>

                <Snackbar open={open} autoHideDuration={4000} onClose={handleClose}>
                <Alert onClose={handleClose} severity="error" sx={{width: '100%'}}>
                You must selected one file of each category and fill the prediction size and noise!
                </Alert>
                </Snackbar>
                </div>:
                <div>
                    <h2>The transformation of the file has finished</h2>
                </div>

            }</>}
            
        </ToolbarWrapper>
    )
}
