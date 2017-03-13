import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/database/index/transformer");
import getTransformersCommand = require("commands/database/transformers/getTransformersCommand");
import saveTransformerLockModeCommand = require("commands/database/transformers/saveTransformerLockModeCommand");
import appUrl = require("common/appUrl");
import deleteTransformerConfirm = require("viewmodels/database/transformers/deleteTransformerConfirm");
import app = require("durandal/app");
import database = require("models/resources/database");
import eventsCollector = require("common/eventsCollector");

type transformerGroup = {
    entityName: string;
    transformers: KnockoutObservableArray<transformer>;
    groupHidden: KnockoutObservable<boolean>;
}

class transformers extends viewModelBase {

    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    transformersGroups = ko.observableArray<transformerGroup>();
    selectedTransformersName = ko.observableArray<string>();
    searchText = ko.observable<string>();
    lockModeCommon: KnockoutComputed<string>;
    selectionState: KnockoutComputed<checkbox>;

    globalLockChangesInProgress = ko.observable<boolean>(false);
    localLockChangesInProgress = ko.observableArray<string>([]);

    constructor() {
        super();

        this.bindToCurrentInstance("deleteTransformer", "deleteSelectedTransformers",
            "unlockTransformer", "lockTransformer",
            "unlockSelectedTransformers", "lockSelectedTransformers");

        this.initObservables();
    }

    private initObservables() {
        this.searchText.throttle(200).subscribe(() => this.filterTransformers());

        this.lockModeCommon = ko.computed(() => {
            const selectedTransformers = this.getSelectedTransformers();
            if (selectedTransformers.length === 0)
                return "None";

            const firstLockMode = selectedTransformers[0].lockMode();
            for (let i = 1; i < selectedTransformers.length; i++) {
                if (selectedTransformers[i].lockMode() !== firstLockMode) {
                    return "Mixed";
                }
            }
            return firstLockMode;
        });

        this.selectionState = ko.pureComputed<checkbox>(() => {
            var selectedCount = this.selectedTransformersName().length;
            if (selectedCount === this.getAllTransformers().length)
                return checkbox.Checked;
            if (selectedCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });
    }

    activate(args: any) {
        super.activate(args);
        return this.fetchTransformers(this.activeDatabase());
    }

    attached() {
        super.attached();
        this.updateHelpLink("OWRJLV");
        ko.postbox.publish("SetRawJSONUrl", appUrl.forTransformersRawData(this.activeDatabase()));
    }

    private filterTransformers() {
        const filterLower = this.searchText().toLowerCase();
        this.selectedTransformersName([]);

        this.transformersGroups()
            .forEach(transformerGroup => {
                let hasAnyInGroup = false;

                transformerGroup.transformers().forEach(trans => {
                    const match = trans.name().toLowerCase().indexOf(filterLower) >= 0;
                    trans.filteredOut(!match);

                    if (match) {
                        hasAnyInGroup = true;
                    }
                });

                transformerGroup.groupHidden(!hasAnyInGroup);
            });
    }

    private fetchTransformers(db: database) {
        return new getTransformersCommand(db)
            .execute()
            .done((transformers: Raven.Client.Documents.Transformers.TransformerDefinition[]) => {
                transformers
                    .map(t => new transformer(t))
                    .forEach(i => this.putTransformerIntoGroup(i));
            });
    }

    afterClientApiConnected(): void {
        const changesApi = this.changesContext.databaseChangesApi();
        this.addNotification(changesApi.watchAllTransformers((e) => this.processTransformerEvent(e)));
    }

    private processTransformerEvent(e: Raven.Client.Documents.Changes.TransformerChange) {
        if (e.Type === "TransformerRemoved") {
            const existingTransformer = this.findTransformerByName(e.Name);
            if (existingTransformer) {
                this.removeTransformersFromAllGroups([existingTransformer]);    
            }
        } else {
            this.fetchTransformers(this.activeDatabase());
        }
    }

    private findTransformerByName(transformerName: string): transformer {
        const transformsGroups = this.transformersGroups();
        for (let i = 0; i < transformsGroups.length; i++) {
            const group = transformsGroups[i];

            const transformers = group.transformers();
            for (let j = 0; j < transformers.length; j++) {
                if (transformers[j].name() === transformerName) {
                    return transformers[j];
                }
            }
        }

        return null;
    }

    private putTransformerIntoGroup(trans: transformer) {
        eventsCollector.default.reportEvent("transformers", "put-into-group");

        const groupName = trans.name().split("/")[0];
        const group = this.transformersGroups().find(g => g.entityName === groupName);

        if (group) {
            const existingTrans = group.transformers().find((cur: transformer) => cur.name() === trans.name());

            if (!existingTrans) {
                group.transformers.push(trans);
            }
        } else {
            this.transformersGroups.push({
                entityName: groupName,
                transformers: ko.observableArray([trans]),
                groupHidden: ko.observable(false)
            });
        }
    }

    deleteSelectedTransformers() {
        eventsCollector.default.reportEvent("transformers", "delete-selected");
        this.promptDeleteTransformers(this.getSelectedTransformers());
    }

    private getAllTransformers(): transformer[] {
        var all: transformer[] = [];
        this.transformersGroups().forEach(g => all.push(...g.transformers()));
        return _.uniq(all);
    }

    private getSelectedTransformers(): Array<transformer> {
        const selectedTransformers = this.selectedTransformersName();
        return this.getAllTransformers().filter(x => _.includes(selectedTransformers, x.name()));
    }

    deleteTransformer(transformerToDelete: transformer) {
        eventsCollector.default.reportEvent("transformers", "delete");
        this.promptDeleteTransformers([transformerToDelete]);
    }

    private promptDeleteTransformers(transformers: Array<transformer>) {
        const db = this.activeDatabase();
        const deleteViewmodel = new deleteTransformerConfirm(transformers.map(i => i.name()), db);
        deleteViewmodel.deleteTask.done(() => this.removeTransformersFromAllGroups(transformers));
        app.showBootstrapDialog(deleteViewmodel);
    }

    private removeTransformersFromAllGroups(transformers: Array<transformer>) {
        this.transformersGroups().forEach(transGroup => transGroup.transformers.removeAll(transformers));
        this.transformersGroups.remove((item: transformerGroup) => item.transformers().length === 0);
    }

    unlockSelectedTransformers() {
        this.setLockModeSelectedTransformers("Unlock", "Unlock");
    }

    lockSelectedTransformers() {
        this.setLockModeSelectedTransformers("LockedIgnore", "Lock");
    }

    private setLockModeSelectedTransformers(lockModeString: Raven.Client.Documents.Transformers.TransformerLockMode,
        localModeString: string) {

        if (this.lockModeCommon() === lockModeString)
            return;

        this.confirmationMessage("Are you sure?", `Do you want to ${localModeString} selected transformers?`)
            .done(can => {
                if (can) {
                    eventsCollector.default.reportEvent("transformers", "lock-selected");

                    this.globalLockChangesInProgress(true);

                    const transformers = this.getSelectedTransformers();

                    new saveTransformerLockModeCommand(transformers, lockModeString, this.activeDatabase())
                        .execute()
                        .done(() => transformers.forEach(t => t.lockMode(lockModeString)))
                        .always(() => this.globalLockChangesInProgress(false));
                }
            });
    }

    lockTransformer(t: transformer) {
        eventsCollector.default.reportEvent("transformers", "lock");

        this.updateTransformerLockMode(t, "LockedIgnore");
    }

    unlockTransformer(t: transformer) {
        eventsCollector.default.reportEvent("transformers", "unlock");

        this.updateTransformerLockMode(t, "Unlock");
    }

    private updateTransformerLockMode(t: transformer, lockMode: Raven.Client.Documents.Transformers.TransformerLockMode) {
        if (t.lockMode() !== lockMode) {
            this.localLockChangesInProgress.push(t.name());

            new saveTransformerLockModeCommand([t], lockMode, this.activeDatabase())
                .execute()
                .done(() => t.lockMode(lockMode))
                .always(() => this.localLockChangesInProgress.remove(t.name()));
        }
    }

    toggleSelectAll() {
        eventsCollector.default.reportEvent("transformers", "toggle-select-all");

        const selectedCount = this.selectedTransformersName().length;

        if (selectedCount > 0) {
            this.selectedTransformersName([]);
        } else {
            const allTransformerNames = this.getAllTransformers().map(idx => idx.name());
            this.selectedTransformersName(allTransformerNames);
        }
    }
}

export = transformers;
