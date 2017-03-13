import viewModelBase = require("viewmodels/viewModelBase");
import extensions = require("common/extensions");
import getReplicationsCommand = require('commands/database/replication/getReplicationsCommand');
import explainReplicationCommand = require("commands/database/replication/explainReplicationCommand");
import eventsCollector = require("common/eventsCollector");

class statusDebugExplainReplication extends viewModelBase {
    destinations = ko.observable<replicationDestinationDto[]>([]);
    selectedDestination = ko.observable<replicationDestinationDto>();
    documentId = ko.observable<string>();
    documentIdSearchResults = ko.observableArray<string>();
    explanation = ko.observable<replicationExplanationForDocumentDto>();

    constructor() {
        super();
        extensions.install();
        this.documentId.throttle(250).subscribe(search => this.fetchDocSearchResults(search));
    }

    canActivate(args: any) {
        super.canActivate(args);

        var deferred = $.Deferred();

        $.when(this.fetchReplicationDestinations())
            .always(() => deferred.resolve({ can: true }));

        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
    }

    private fetchReplicationDestinations() {
        return new getReplicationsCommand(this.activeDatabase())
            .execute()
            .done((destinations: configurationDocumentDto<replicationsDto>) => {
                this.destinations(destinations.MergedDocument.Destinations);
            });
    }

    buttonEnabled = ko.computed(() => {
        var destionationSelected = this.selectedDestination() != null;
        var documentSelected = this.documentId();
        return destionationSelected && (documentSelected != null && documentSelected.length > 0);
    });

    setSelectedDestination(destination: replicationDestinationDto) {
        this.selectedDestination(destination);
    }

    selectDocument(data: documentMetadataDto) {
        this.documentId((<any>data)['@metadata']['@id']);
    }

    fetchDocSearchResults(query: string) {
        if (query.length >= 2) {
            /* TODO
            new getDocumentsMetadataByIDPrefixCommand(query, 10, this.activeDatabase())
                .execute()
                .done((results: queryResultDto<string>) => {
                    if (this.documentId() === query) {
                        this.documentIdSearchResults(results.Results);
                    }
                });*/
        } else if (query.length == 0) {
            this.documentIdSearchResults.removeAll();
        }
    }

    explain() {
        eventsCollector.default.reportEvent("replicaton", "explain");
        new explainReplicationCommand(this.activeDatabase(), this.documentId(), this.selectedDestination().Url, this.selectedDestination().Database)
            .execute()
            .done(result => this.explanation(result)); 
    }

    selectedDestionationText = ko.computed(() => {
        var dest = this.selectedDestination();
        if (dest) {
            return dest.Database + ' on ' + dest.Url;
        } else {
            return 'select destination';
        }
    });
}

export = statusDebugExplainReplication;
