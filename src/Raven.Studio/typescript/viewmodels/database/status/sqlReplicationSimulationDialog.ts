import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import simulateSqlReplicationCommand = require("commands/database/sqlReplication/simulateSqlReplicationCommand");
import database = require("models/resources/database");
import dialog = require("plugins/dialog");
import sqlReplication = require("models/database/sqlReplication/sqlReplication");
import sqlReplicationSimulatedCommand = require ("models/database/sqlReplication/sqlReplicationSimulatedCommand");

class sqlReplicationSimulationDialog extends dialogViewModelBase {
    simulationResults = ko.observableArray<sqlReplicationSimulatedCommand>([]);
    rolledBackTransactionPassed = ko.observable<boolean>(false);
    documentAutocompletes = ko.observableArray<string>();
    lastSearchedDocumentID = ko.observable<string>("");
    isAutoCompleteVisible: KnockoutComputed<boolean>;
    rolledbackTransactionPerformed = ko.observable<boolean>(false);
    lastAlert = ko.observable<string>("");
    waitingToAnswer = ko.observable<boolean>(false);
    
    constructor(private db: database, private simulatedSqlReplication: sqlReplication, private documentId: KnockoutObservable<string>) {
        super();
        this.documentId.throttle(250).subscribe(search => this.fetchDocumentIdAutocompletes(search));
        this.isAutoCompleteVisible = ko.computed(() => {
            return this.lastSearchedDocumentID() !== this.documentId() && 
                (this.documentAutocompletes().length > 1 || this.documentAutocompletes().length == 1 && this.documentId() !== this.documentAutocompletes()[0]);
        });
    }


    toggleCommandParamView(command: sqlReplicationSimulatedCommand) {
        command.showParamsValues.toggle();
    }

    getResults(performRolledbackTransaction: boolean) {
        this.lastSearchedDocumentID(this.documentId());
        this.waitingToAnswer(true);
        new simulateSqlReplicationCommand(this.db, this.simulatedSqlReplication, this.documentId(), performRolledbackTransaction)
            .execute()
            .done((result: sqlReplicationSimulationResultDto) => {
                if (!!result.Results) {
                    this.simulationResults(result.Results.map(x=> x.Commands).reduce((x, y) => x.concat(y)).map(x => { return new sqlReplicationSimulatedCommand(!performRolledbackTransaction, x); }));
                } else {
                    this.simulationResults([]);
                }

                /* TODO
                this.rolledBackTransactionPassed(!result.LastAlert);

                if (!!result.LastAlert) {
                    this.lastAlert(result.LastAlert.Exception);
                } else {
                    this.lastAlert("");
                }*/
                this.rolledbackTransactionPerformed(performRolledbackTransaction);

            })
            .fail(() => {
                this.simulationResults([]);
                this.rolledBackTransactionPassed(false);
            })
            .always(() => this.waitingToAnswer(false));
    }

    // overrid dialogViewModelBase shortcuts behavior
   attached() {
       super.attached();
       $("#docIdInput").focus();
   }

    fetchDocumentIdAutocompletes(query: string) {
        if (query.length >= 2) {
            /* TODO
            new getDocumentsMetadataByIDPrefixCommand(query, 10, this.db)
                .execute()
                .done((results: documentsMetadataOnlyResponse[]) => {
                    if (this.documentId() === query) {
                        if (results.length === 1 && this.documentId() == results[0]) {
                            this.documentAutocompletes.removeAll();
                            return;
                        }
                        this.documentAutocompletes(results);
                    }
                });*/
        } else if (query.length == 0) {
            this.documentAutocompletes.removeAll();
        }
    }

    documentIdSubmitted(submittedDocumentId: string) {
        this.documentId(submittedDocumentId);
        $('#docIdInput').focus();
        this.getResults(false);
    }

    /* TODO
    getDocCssClass(doc: documentMetadataDto) {
        return collection.getCollectionCssClass((<any>doc)['@metadata']['@collection'], this.db);
    }*/

    keyPressedOnDocumentAutocomplete(doc: documentMetadataDto, event: JQueryKeyEventObject) {
        if (event.keyCode == 13 && !!doc) {
            var docId = !!(<any>doc)['@metadata'] ? (<any>doc)['@metadata']['@id'] : null;
            if (!!docId) {
                this.documentId(docId);
            }
        }
    }

    cancel() {
        dialog.close(this);
    }


}

export = sqlReplicationSimulationDialog;
