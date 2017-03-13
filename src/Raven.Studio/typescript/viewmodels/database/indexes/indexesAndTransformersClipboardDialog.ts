import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import getIndexesDefinitionsCommand = require("commands/database/index/getIndexesDefinitionsCommand");
import getTransformersCommand = require("commands/database/transformers/getTransformersCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");


//TODO: we will probably split this class into indexes and transformers
class indexesAndTransformersClipboardDialog extends dialogViewModelBase {

    json = ko.observable<string>("");
    indexes = ko.observableArray<Raven.Client.Documents.Indexes.IndexDefinition>([]);
    //TODO: transformers = ko.observableArray<transformerDto>([]);
    pasteDeferred = $.Deferred();


    constructor(private db: database, private isPaste: boolean = false, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
        aceEditorBindingHandler.install();
    }

    canActivate(args: any): any {
        if (this.isPaste) {
            return true;
        } else {
            var canActivateResult = $.Deferred();
            var getIndexDefinitionsPromise =
                new getIndexesDefinitionsCommand(this.db)
                    .execute()
                    .done((results) => {
                        this.indexes(results);
                    });
            var getTransformersPromise =
                new getTransformersCommand(this.db)
                    .execute();/* TODO
                    .done((results: transformerDto[]) => {
                        this.transformers(results);
                    });*/
            $.when<any>(getTransformersPromise, getIndexDefinitionsPromise)
                .then(() => {
                    canActivateResult.resolve({ can: true });
                    var prettifySpacing = 4;
                    this.json(JSON.stringify({
                        Indexes: this.indexes(),
                        //TODO:Transformers: this.transformers()
                    }, null, prettifySpacing));
                },
                () => {
                canActivateResult.reject();
            });
            return canActivateResult;
        }
    }

    setInitialFocus() {
        // Overrides the base class' setInitialFocus and does nothing.
        // Doing nothing because we will focus the Ace Editor when it's initialized.
    }
     
    enterKeyPressed(): boolean {
        // Overrides the base class' enterKeyPressed. Because the user might
        // edit the JSON, or even type some in manually, enter might really mean new line, not Save changes.
        if (!this.isPaste) {
            return super.enterKeyPressed();
        } else {
            this.saveAll();
        }

        return true;
    }

    saveAll() {
        /* TODO
        if (this.isPaste && this.json()) {
            var indexesAndTransformers: { Indexes: indexDefinitionListItemDto[]; Transformers: transformerDto[] };
            var indexesDefinitions: indexDefinitionDto[] = [];
            var transformersDefinitions: savedTransformerDto[] = [];

            try {
                indexesAndTransformers = JSON.parse(this.json());
                if (indexesAndTransformers.Indexes && indexesAndTransformers.Indexes.length > 0) {
                    indexesDefinitions.pushAll(indexesAndTransformers.Indexes.map((index: indexDefinitionListItemDto) => {
                        return index.definition;
                    }));
                }

                if (indexesAndTransformers.Transformers && indexesAndTransformers.Transformers.length > 0) {
                    transformersDefinitions.pushAll(indexesAndTransformers.Transformers.map((transformer: transformerDto) => {
                        return {
                             Transformer: {
                                Name: transformer.name,
                                TransformResults: transformer.definition.TransformResults,
                                LockMode: transformer.definition.LockMode
                            }
                        }
                    }));
                }
                if (indexesDefinitions.length === 0 && transformersDefinitions.length === 0) {
                    throw "No indexes or transformers found in json string";
                }
            } catch (e) {
                this.pasteDeferred.reject();
                messagePublisher.reportError("Index paste failed, invalid json string", e);
            }

            var allOperationsPromises: Array<JQueryPromise<any>> = [];
            var succeededIndexes: string[] = [];
            var failedIndexes: string[] = [];
            var succeededTransformers: string[] = [];
            var failedTransformers: string[] = [];

            if (indexesDefinitions.length > 0) {
                indexesDefinitions.forEach((index: indexDefinitionDto) => {
                    var curDeferred = $.Deferred();
                    allOperationsPromises.push(curDeferred);
                    new saveIndexDefinitionCommand(index, this.db)
                        .execute()
                        .done(() => succeededIndexes.push(index.Name))
                        .fail(() => failedIndexes.push(index.Name))
                        .always(() => curDeferred.resolve());

                });
            }
            
            if (transformersDefinitions.length > 0) {
                transformersDefinitions.forEach((transformer: savedTransformerDto) => {
                    var curDeferred = $.Deferred();
                    allOperationsPromises.push(curDeferred);
                    new saveTransformerCommand(new transformerDefinition().initFromSave(transformer), this.db)
                        .execute()
                        .done(() => {
                            succeededTransformers.push(transformer.Transformer.Name);
                        })
                        .fail(() => {
                            failedTransformers.push(transformer.Transformer.Name);
                        })
                        .always(() => curDeferred.resolve());
                });
            }

            $.when.apply($, allOperationsPromises)
                .always(() => this.summarize(succeededIndexes, failedIndexes, succeededTransformers, failedTransformers));
        } else {
            this.close();
        }*/
    }

    summarize(succeededIndexes: string[], failedIndexes: string[], succeededTransformers: string[], failedTransformers:string[]) {
        var summaryText = "";
        if (succeededIndexes.length > 0) {
            summaryText += "Succeeded Indexes: " + succeededIndexes.length + "\n";
        }

        if (failedIndexes.length > 0) {
            summaryText += "Failed Indexes: \n" + failedIndexes.join("\n") + "\n";
        }

        if (succeededTransformers.length > 0) {
            summaryText += "Succeeded Transformers: " + succeededTransformers.length + "\n";
        }

        if (failedTransformers.length > 0) {
            summaryText += "Failed Transformers: \n" + failedTransformers.join("\n") + "\n";
        }

        this.pasteDeferred.resolve(summaryText);

        this.close();
    }

    close() {
        dialog.close(this);
    }
}

export = indexesAndTransformersClipboardDialog;
