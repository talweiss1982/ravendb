import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import changesContext = require("common/changesContext");
import moment = require("moment");
import changeSubscription = require("common/changeSubscription");
import tableNavigationTrait = require("common/tableNavigationTrait");

class indexErrors extends viewModelBase {

    allIndexErrors = ko.observableArray<serverErrorDto>();
    hasFetchedErrors = ko.observable(false);
    selectedIndexError = ko.observable<serverErrorDto>();
    now = ko.observable<moment.Moment>();
    updateNowTimeoutHandle = 0;

    tableNavigation: tableNavigationTrait<serverErrorDto>;

    constructor() {
        super();

        this.updateCurrentNowTime();

        this.tableNavigation = new tableNavigationTrait<serverErrorDto>("#indexErrorsTableContainer", this.selectedIndexError, this.allIndexErrors, i => "#indexErrorsTableContainer table tbody tr:nth-child(" + (i + 1) + ")");
    }

    afterClientApiConnected(): void {
        const changesApi = this.changesContext.databaseChangesApi();
        this.addNotification(changesApi.watchAllIndexes(() => this.fetchIndexErrors()));
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('ABUXGF');
        this.fetchIndexErrors();
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
    }

    fetchIndexErrors(): JQueryPromise<any> { //TODO: use type
        var db = this.activeDatabase();
        if (db) {
            /* TODO
            // Index errors are actually the .ServerErrors returned from the database statistics query.
            return new getDatabaseStatsCommand(db)
                .execute()
                .done((stats: databaseStatisticsDto) => {
                    stats.Errors.forEach((e: any) => e['TimestampHumanized'] = this.createHumanReadableTime(e.Timestamp));
                    this.allIndexErrors(stats.Errors);
                    this.hasFetchedErrors(true);
                });*/
        }

        return null;
    }

    selectIndexError(indexError: serverErrorDto) {
        this.selectedIndexError(indexError);
    }

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        if (time) {
            // Return a computed that returns a humanized string based off the current time, e.g. "7 minutes ago".
            // It's a computed so that it updates whenever we update this.now field.
            return ko.computed(() => {
                var dateMoment = moment(time);
                var agoInMs = dateMoment.diff(this.now());
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            });
        }

        return ko.computed(() => time);
    }

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }
}

export = indexErrors; 
