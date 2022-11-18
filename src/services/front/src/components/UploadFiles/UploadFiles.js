import React, {useState} from 'react'
import ToolbarWrapper from '../Toolbar/Toolbar'
import { uploadFiles } from '../../service';
import styles from './uploadFiles.module.css';

export default function UploadFiles() {
    const [file, setFile] = useState(null);
    const NAME_OF_CLUSTER = process.env.REACT_APP_NAME_OF_CLUSTER;

    const uploadFile = async(event) => {
        console.log('On upload ', event.target.files[0]);
        let file =  event.target.files[0];
        setFile(file);

    }


    const uploadAllFiles = async(event) => {
        event.preventDefault();
        let formData = new FormData();
        formData.append('uploadedFile', file);

        try{
            const response = await uploadFiles(formData);
            console.log('RESPONSE', response);
        }catch(error){
            console.log('ERROR ', error);
        }

    }

  return (
    <ToolbarWrapper>
          <form method='post' >
            <div className={styles.UploadFilesWrapper}>
              <div className={styles.Cluster}>
                <p>Upload files for the {NAME_OF_CLUSTER}</p>
                <input type="file" name="uploadedFile" onChange={uploadFile} onLoad={uploadFile}  />
              </div>           
          </div>
          <div>
              <button type="submit" onClick={uploadAllFiles}>Upload All Files</button>
          </div>
           
        </form>
    </ToolbarWrapper>
  )
}
