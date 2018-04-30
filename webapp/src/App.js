import React, { Component } from 'react';
import MuiThemeProvider from 'material-ui/styles/MuiThemeProvider';
import MenuComponent from './components/Menu';
import logo from './logo.svg';
import './App.css';

class App extends Component {
  render() {
    return (
      <div className="App">
        <header className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h1 className="App-title">Welcome to Committed</h1>
        </header>

        <MuiThemeProvider>
          <MenuComponent />
        </MuiThemeProvider>
      </div>
    );
  }
}

export default App;
