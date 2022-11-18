import React from 'react'
import { Link } from "react-router-dom";
import styles from './toolbar.module.css'

export default function ToolbarWrapper({children}) {
  return (
    <div className={styles.ToolbarContainer}>
        <div className={styles.NavWrapper}>
            <div className={styles.Nav}>
                <Link to="/"><p>Home</p></Link>
            </div>
            <div className={styles.Nav}>
                <Link to="/upload-files"><p>Upload Files</p></Link>
            </div>
            <div className={styles.Nav}>
                <Link to="/all-files"><p>All files</p></Link>
            </div>
            <div className={styles.Nav}>
                <Link to="/hdfs"><p>HDFS</p></Link>
            </div>
            <div className={styles.Nav}>
                <Link to="/request-matched-data"><p>Request Matched Data</p></Link>
            </div>
        </div>
        <div className={styles.ChildContainer}>
            {children}
        </div>
    </div>
  )
}
