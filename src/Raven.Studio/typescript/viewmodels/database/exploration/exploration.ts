import viewModelBase = require("viewmodels/viewModelBase");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import appUrl = require("common/appUrl");
import dataExplorationRequest = require("models/database/query/dataExplorationRequest");
import dataExplorationCommand = require("commands/database/query/dataExplorationCommand");
import collectionsStats = require("models/database/documents/collectionsStats");

import eventsCollector = require("common/eventsCollector");

class exploration extends viewModelBase {

    appUrls: any;
    collections = ko.observableArray<string>([]);
    isBusy = ko.observable<boolean>(false);
    explorationRequest = dataExplorationRequest.empty();
    queryResults = ko.observable<any>(); //TODO: use type
    isLoading = ko.observable<boolean>(false);
    dataLoadingXhr = ko.observable<any>();

    selectedCollectionLabel = ko.computed(() => this.explorationRequest.collection() || "Select a collection");

    runEnabled = ko.computed(() => !!this.explorationRequest.collection());

    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
        aceEditorBindingHandler.install();
    }

    canActivate(args: any): any {
        var deffered = $.Deferred();

        new getCollectionsStatsCommand(this.activeDatabase())
            .execute()
            .done((collectionStats: collectionsStats) => {
                this.collections(collectionStats.collections.map(x => x.name));
            })
            .always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    activate(args?: string) {
        super.activate(args);

        this.updateHelpLink("FP59PJ");
    }

    exportCsv() {
        eventsCollector.default.reportEvent("exploration", "export-csv");
        var db = this.activeDatabase();
        var url = new dataExplorationCommand(this.explorationRequest.toDto(), db).getCsvUrl();
        this.downloader.download(db, url);
    }

    runExploration() {
        eventsCollector.default.reportEvent("exploration", "run");
        this.isBusy(true);
        var requestDto = this.explorationRequest.toDto();

        var command = new dataExplorationCommand(requestDto, this.activeDatabase());
        command.execute()
            .done((results: Raven.Client.Documents.Queries.QueryResult<Array<any>>) => { //TODO: avoid using any? 
                // TODO if (results.Error) {
                    //TODO:messagePublisher.reportError("Unable to execute query", results.Error);
                //TODO: } else {
                /* TODO
                    var mainSelector = new pagedResult(results.Results.map(d => new document(d)), results.Results.length, results);
                    var resultsFetcher = (skip: number, take: number) => {
                        var slicedResult = new pagedResult(mainSelector.items.slice(skip, Math.min(skip + take, mainSelector.totalResultCount)), mainSelector.totalResultCount);
                        return $.Deferred().resolve(slicedResult).promise();
                    };*/
                    /* TODO var resultsList = new pagedList(resultsFetcher);
                    this.queryResults(resultsList); */
                //TODO: }
            })
            .always(() => {
                this.isBusy(false);
            });

        this.dataLoadingXhr(command.xhr);
    }

    killTask() {
        eventsCollector.default.reportEvent("exploration", "kill");
        var xhr = this.dataLoadingXhr();
        if (xhr) {
            xhr.abort();
        }
        this.isBusy(false);
        this.queryResults(null);
    }

}

export = exploration;
