import React from 'react';
import { BrowserRouter as Router, Route, NavLink } from 'react-router-dom'
import TopicsComponent from './Topics';
import SyncablesComponent from './Syncables';
import Drawer from 'material-ui/Drawer';
import MenuItem from 'material-ui/MenuItem';
import {GridList, GridTile} from 'material-ui/GridList';
import AppBar from 'material-ui/AppBar';
import StorageIcon from 'material-ui/svg-icons/device/storage';
import {white} from 'material-ui/styles/colors';
import IconButton from 'material-ui/IconButton';

const MenuComponent = () => (
    <Router>
        <div>
            <Drawer open="true" docked="true">
                <AppBar title="Committed DB" iconElementLeft={<IconButton><StorageIcon color={white}/></IconButton>}/>
                <NavLink to="/topics"><MenuItem>Topics</MenuItem></NavLink>
                <NavLink to="/syncables"><MenuItem>Syncables</MenuItem></NavLink>
            </Drawer>

            <GridList>
                <GridTile>
                    <Route path="/topics" component={TopicsComponent} />
                    <Route path="/syncables" component={SyncablesComponent} />
                </GridTile>
            </GridList>
        </div>
    </Router>
);

export default MenuComponent;