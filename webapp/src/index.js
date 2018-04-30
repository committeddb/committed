// The is required to support generators in create-react-app
import regeneratorRuntime from "regenerator-runtime";
import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import registerServiceWorker from './registerServiceWorker';

global.regeneratorRuntime = regeneratorRuntime;

ReactDOM.render(
    <App />,
    document.getElementById('root')
);
registerServiceWorker();
