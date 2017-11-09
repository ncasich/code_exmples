import React, {Component} from 'react';
import PanelPrimary from '../../../../../../../react/templates/panels/PanelPrimary';
import PanelControls from '../../../../../../../react/templates/panels/PanelControls';
import CompanyStore from '../../../../../../../react/stores/CompanyStore';
import CompanyActions from '../../../../../../../react/actions/CompanyActions';

function getStateFromFlux() {
    return CompanyStore.getCompany()
}

const CompanySingle = React.createClass({
    getInitialState(){

        return getStateFromFlux();
    },

    componentWillMount() {
        this.type = 'COMPANY_DATA';
        this.event = this.type + '_LOADED';
        CompanyActions.loadCompanyData(this.props.params.id);
    },

    componentDidMount() {
        CompanyStore.addChangeListener(this.event, this._onChange);
    },

    componentWillUnmount() {
        CompanyStore.removeChangeListener(this.event, this._onChange);
    },

    _onChange() {
        this.setState(getStateFromFlux());
    },

    render() {
        let route = this.props.routes[this.props.routes.length - 1];
        let company = this.state.company;
        if (route.path == 'edit') {
            var children = React.Children.map(this.props.children, function (child) {
                return React.cloneElement(child, {
                    company: company
                })
            });
            return <div className="container">{children}</div>
        } else {
            return (
                <PanelPrimary title={
                    <PanelControls
                        title={company.title}
                        item_id={company.id}
                        controller="company"/>}>
                    <table className="table table-stripped">
                        <tbody>
                        <tr>
                            <th>Домашняя страница</th>
                            <th>Регион</th>
                        </tr>
                        <tr>
                            <td><a href={company.link}>{company.link}</a></td>
                            <td>{company.location}</td>
                        </tr>
                        <tr>
                            <td colSpan="2" className="text-justify">
                                <i className="fa fa-info-circle" aria-hidden="true"/>&nbsp;&nbsp;&nbsp;
                                {company.info}
                            </td>
                        </tr>
                        </tbody>
                    </table>
                </PanelPrimary>
            )
        }
    }
});

module.exports = CompanySingle;