import './App.css';
import React, { useState } from 'react';
import ShowFiles from "./components/ShowFiles/ShowFiles.js";
import UploadFiles from "./components/UploadFiles/UploadFiles.js";
import Home from './components/Home/Home';
import HdfsShowFiles from "./components/HdfsShowFiles/ShowFiles.js";
import RequestMatchedData from "./components/RequestMatchedData/RequestMatchedData.js";
import {Helmet} from "react-helmet";

import {
  BrowserRouter as Router, 
  Routes, 
  Route
} from 'react-router-dom';


function App() {
  const [files, setFiles] = useState([
    {
      fileName: 'File 1',
      columns: ['Column A', 'Column B']
    },
    {
      fileName: 'File 2',
      columns: ['Column A', 'Column B', 'Column C']
    },
    {
      fileName: 'File 3',
      columns: ['Column A']
    }
  ]);
  const [filesA, setFilesA] = useState(null);
  const [filesB, setFilesB] = useState(null);
  const [joinedFiles, setJoinedFiles] = useState(null);
  const [matchedFiles, setMatchedFiles] = useState(null);
  const [transformedFiles, setTransformedFiles] = useState(null);
  const NAME_OF_CLUSTER = process.env.REACT_APP_NAME_OF_CLUSTER;

  const uploadCompletedFiles = () => {
    /* State */

    /* Response add file list to state */
  }

  return (
    <div className="App">
      <Helmet>
          <meta charSet="utf-8" />
          <title> ICSOC2022 - {NAME_OF_CLUSTER}</title>
      </Helmet>
      <Router>
        <Routes>
          <Route path="/" element={<Home/>} />
          <Route path="/all-files" element={<ShowFiles filesA={filesA} setFilesA={setFilesA}/>} />
          <Route path="/hdfs" element={<HdfsShowFiles joinedFiles={joinedFiles} setJoinedFiles={setJoinedFiles} matchedFiles={matchedFiles} setMatchedFiles={setMatchedFiles} transformedFiles={transformedFiles} setTransformedFiles={setTransformedFiles}/>} />
          <Route path="/upload-files" element={<UploadFiles/>} />
          <Route path="/request-matched-data" element={<RequestMatchedData matchedFiles={matchedFiles} setMatchedFiles={setMatchedFiles}/>} />
        </Routes>
    </Router>
    </div>
  );
}

export default App;
