import React from 'react'
import ToolbarWrapper from '../Toolbar/Toolbar';

export default function Home() {

    const NAME_OF_CLUSTER = process.env.REACT_APP_NAME_OF_CLUSTER;

  return (
    <ToolbarWrapper>
        <p> {NAME_OF_CLUSTER} </p>
    </ToolbarWrapper>

  )
}
