module.exports = {
    path: ':id',

    getChildRoutes(partialNextState, cb) {
        require.ensure([], (require) => {
            cb(null, [
                require('./routes/Edit'),
            ])
        }, 'company-single-subroutes')
    },

    getComponent(nextState, cb) {
        require.ensure([], (require) => {
            cb(null, require('./components/company-single'))
        }, 'company-single')
    }
};