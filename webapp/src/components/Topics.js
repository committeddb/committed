import React from 'react';
import {List, ListItem} from 'material-ui/List';

const API = '/cluster/topics';

export default class TopicsComponent extends React.Component {
    constructor(props) {
        super(props);
        this.state = {Topics: []}
    }

    componentDidMount() {
        fetch(API)
            .then(response => isJson(response) ? response.json() : {Topics: ["No Topics"]})
            .then(data => this.setState(data));
    }
    
    render() {
        return (
            <List>
                {this.state.Topics.map(function(topic) {
                    return <ListItem primaryText={topic} />;
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