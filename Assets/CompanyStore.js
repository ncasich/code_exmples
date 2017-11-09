import React from 'react';
import {Link} from 'react-router';
import {EventEmitter} from 'events';
import {TableRow, TableRowColumn} from 'material-ui/Table';
import Delete from 'material-ui/svg-icons/action/delete-forever';
import Edit from 'material-ui/svg-icons/editor/mode-edit';

import AppDispatcher from '../dispatcher/AppDispatcher.js';

let _companies = {companies: []};
let _company = {company:{}};
let _error = null;

function getState(data) {
    _companies.current = data.current;
    _companies.last = data.last;
    _companies.companies = [];
    for (let id in data.companies) {
        _companies.companies.push(

            <TableRow key={id}>
                <TableRowColumn><Link to={'company/' + id}>{data.companies[id].title}</Link></TableRowColumn>
                <TableRowColumn><a href={data.companies[id].link}>{data.companies[id].link}</a></TableRowColumn>
                <TableRowColumn>{data.companies[id].location}</TableRowColumn>
                <TableRowColumn>{data.companies[id].info}</TableRowColumn>
                <TableRowColumn><Edit/></TableRowColumn>
                <TableRowColumn><Delete/></TableRowColumn>
            </TableRow>

        );
    }
    return _companies;
}

const CompanyStore = Object.assign({}, EventEmitter.prototype, {
    getCompanies(){
        return _companies;
    },
    getCompany(){
        return _company;
    },
    emitChange(event){
        this.emit(event);
    },
    addChangeListener(event, callback){
        this.on(event, callback);
    },
    removeChangeListener(event, callback){
        this.removeListener(event, callback);
    }
});

AppDispatcher.register(action => {
    if (action.type == 'COMPANIES' && action.message.indexOf("_LIST_LOAD")!=-1) {
        if (!action.error) {
            console.info(action.message, action.items);
            _companies = getState(action.items);
        } else {
            console.warn(action.type, action.error);
            _companies = {companies: []};
            _error = action.error;
        }
        CompanyStore.emitChange(action.type + '_LIST_LOADED');
    }

    if (action.type == 'COMPANY_DATA') {
        if (!action.error) {
            console.info(action.message, action.data);
            _company = {company: action.data};

        } else {
            console.warn(action.type, action.error);
            _company = {company:{}};
            _error = action.error;
        }
        CompanyStore.emitChange(action.type + '_LOADED');
    }
});

export default CompanyStore;
