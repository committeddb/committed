import React from 'react';
import {List, ListItem} from 'material-ui/List';

const API = '/cluster/syncables';

export default class SyncablesComponent extends React.Component {
    constructor(props) {
        super(props);
        this.state = {Syncables: []}
    }

    componentDidMount() {
        fetch(API)
            .then(response => isJson(response) ? response.json() : {Syncables: ["No syncables"]})
            .then(data => this.setState(data));
    }
    
    render() {
        return (
            <List>
                {this.state.Syncables.map(function(syncable) {
                    return <pre><ListItem primaryText={syncable} /></pre>;
                })}
            </List>
        );
    }
}

function isJson(response) {
    if (response.ok) {
        return response.headers.get("Content-Type") === "application/json"
    }
    return false
}