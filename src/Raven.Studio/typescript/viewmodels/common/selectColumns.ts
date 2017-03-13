import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class selectColumns extends dialogViewModelBase {
/* TODO:
    private nextTask = $.Deferred<customColumns>();
    nextTaskStarted = false;
    private form: JQuery;
    private activeInput: JQuery;
    private lastActiveInput: JQuery;
    private autoCompleteBase = ko.observableArray<KnockoutObservable<string>>([]);
    private autoCompleteResults = ko.observableArray<KnockoutObservable<string>>([]);
    private completionSearchSubscriptions: Array<KnockoutSubscription> = [];
    private autoCompleterSupport: autoCompleterSupport;
    private columnsNames = ko.observableArray<string>([]);

    constructor(private customColumns: customColumns, private context: any, private database: database, names: string[]) {
        super();
        this.columnsNames(names);
        this.generateCompletionBase();
        this.regenerateBindingSubscriptions();
        this.monitorForNewRows();
        this.autoCompleterSupport = new autoCompleterSupport(this.autoCompleteBase, this.autoCompleteResults, true);
    }

    private generateCompletionBase() {

        this.autoCompleteBase([]);
        this.updateColumnsNames();
        var moduleSource = "var exports = {}; " + this.customFunctions.functions + "; return exports;";
        var exports = new Function(moduleSource)();
        for (var funcName in exports) {
            this.autoCompleteBase().push(ko.observable<string>(funcName + "()"));
        }
    }

    private regenerateBindingSubscriptions() {
        this.completionSearchSubscriptions.forEach((subscription) => subscription.dispose());
        this.completionSearchSubscriptions = [];
        this.customColumns.columns().forEach((column: customColumnParams, index: number) =>
            this.completionSearchSubscriptions.push(
                column.binding.subscribe(this.searchForCompletions.bind(this))
                )
            );
    }

    private monitorForNewRows() {
        this.customColumns.columns().forEach((column: customColumnParams) => column.binding.subscribe(() => {
            this.updateColumnsNames();
        }));
        this.customColumns.columns.subscribe((changes: Array<{ index: number; status: string; value: customColumnParams }>) => {
            this.updateColumnsNames();
            var somethingRemoved: boolean = false;
            changes.forEach((change) => {
                if (change.status === "added") {
                    this.completionSearchSubscriptions.push(
                        change.value.binding.subscribe(this.searchForCompletions.bind(this))
                        );
                }
                else if (change.status === "deleted") {
                    this.completionSearchSubscriptions.push(
                        change.value.binding.subscribe(this.searchForCompletions.bind(this))
                        );
                    somethingRemoved = true;
                }

                if (somethingRemoved) {
                    this.regenerateBindingSubscriptions();
                }
            });
        }, null, "arrayChange");
    }

    private updateColumnsNames() {
        var customColumnsArray = this.customColumns.columns();
        var customColumnsNamesArray = customColumnsArray.map((column: customColumnParams) => column.binding());
        var allColumns = this.columnsNames();
        var arrayWithoutCustomNames = allColumns.filter((name: string) => {
            var filteredNames = customColumnsNamesArray.filter((customName: string) => customName.localeCompare(name) === 0);
            return filteredNames.length === 0;
        });
        var array = arrayWithoutCustomNames.map((name: string) => ko.observable<string>(name));
        this.autoCompleteBase(array);
    }

    attached() {
        super.attached();
        this.form = $("#selectColumnsForm");
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.nextTaskStarted) {
            this.nextTask.reject();
        }
    }

    onExit() {
        return this.nextTask.promise();
    }

    changeCurrentColumns() {
        this.nextTaskStarted = true;
        this.nextTask.resolve(this.customColumns);
        dialog.close(this);
    }

    insertNewRow() {
        this.customColumns.columns.push(customColumnParams.empty());
    }

    deleteRow(row: customColumnParams) {
        this.customColumns.columns.remove(row);
    }

    moveUp(row: customColumnParams) {
        var i = this.customColumns.columns.indexOf(row);
        if (i >= 1) {
            var array = this.customColumns.columns();
            this.customColumns.columns.splice(i - 1, 2, array[i], array[i - 1]);
        }
    }

    moveDown(row: customColumnParams) {
        var i = this.customColumns.columns.indexOf(row);
        if (i >= 0 && i < this.customColumns.columns().length - 1) {
            var array = this.customColumns.columns();
            this.customColumns.columns.splice(i, 2, array[i + 1], array[i]);
        }
    }

    customScheme(val: boolean) {
        if (this.customColumns.customMode() != val) {
            this.customColumns.customMode(val);
        }
    }

    saveAsDefault() {
        if ((<any>this.form[0]).checkValidity() === true) {
            if (this.customColumns.customMode()) {
                eventsCollector.default.reportEvent("custom-columns", "save-as-default", "custom");
                var configurationDocument = new document(this.customColumns.toDto());
                new saveDocumentCommand(this.context, configurationDocument, this.database, false).execute()
                    .done(() => this.onConfigSaved())
                    .fail(() => messagePublisher.reportError("Unable to save configuration!"));
            } else {
                eventsCollector.default.reportEvent("custom-columns", "save-as-default", "auto");
                new deleteDocumentCommand(this.context, this.database).execute().done(() => this.onConfigSaved())
                    .fail(() => messagePublisher.reportError("Unable to save configuration!"));
            }
        } else {
            messagePublisher.reportWarning('Configuration contains errors. Not saving it.');
        }
    }

    onConfigSaved() {
        messagePublisher.reportSuccess('Configuration saved!');
    }

    generateBindingInputId(index: number) {
        return 'binding-' + index;
    }

    enterKeyPressed():boolean {
        var focusedBindingInput = $("[id ^= 'binding-']:focus");
        if (focusedBindingInput.length) {
            // insert first completion
            if (this.autoCompleteResults().length > 0) {
                this.completeTheWord(this.autoCompleteResults()[0]());
            }
            // prevent submitting the form and closing dialog when accepting completion
            return false;
        }
        return super.enterKeyPressed();
    }

    consumeUpDownArrowKeys(columnParams: any, event: KeyboardEvent): boolean {
        if (event.keyCode === 38 || event.keyCode === 40) {
            event.preventDefault();
            return false;
        }
        return true;
    }
    consumeClick(columnParams: any, event: KeyboardEvent): boolean {
        if (columnParams.binding().length === 0) {
            columnParams.binding.valueHasMutated();
            event.preventDefault();
            return false;
        }
        return true;
    }
    searchForCompletions() {
        this.activeInput = $("[id ^= 'binding-']:focus");
        if (this.activeInput.length > 0) {
            this.autoCompleterSupport.searchForCompletions(this.activeInput);
            this.lastActiveInput = this.activeInput;
        }
        else if (!!this.lastActiveInput) {
            this.autoCompleterSupport.searchForCompletions(this.lastActiveInput);
        }
    }

    completeTheWord(selectedCompletion: string) {
        if (this.activeInput.length > 0) {
            this.autoCompleterSupport.completeTheWord(this.activeInput, selectedCompletion, newValue => {
                var columnParams = <customColumnParams> ko.dataFor(this.activeInput[0]);
                columnParams.binding(newValue);
            });
        }
    }*/
}

export = selectColumns;
