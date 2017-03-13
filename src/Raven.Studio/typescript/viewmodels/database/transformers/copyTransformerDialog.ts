import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import getSingleTransformerCommand = require("commands/database/transformers/getSingleTransformerCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class copyTransformerDialog extends dialogViewModelBase {

    transformerJSON = ko.observable("");

    constructor(private transformerName: string, private db: database, private isPaste: boolean = false, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
        aceEditorBindingHandler.install();
    }

    canActivate(args: any): any {
        if (this.isPaste) {
            return true;
        }
        else {
            var canActivateResult = $.Deferred();
            new getSingleTransformerCommand(this.transformerName, this.db)
                .execute();/* TODO
                .done((results: savedTransformerDto) => {
                    var prettifySpacing = 4;
                    var jsonString = JSON.stringify(results, null, prettifySpacing);
                    this.transformerJSON(jsonString);
                    canActivateResult.resolve({ can: true });
                })
                .fail(() => canActivateResult.reject());*/
            canActivateResult.resolve({ can: true });
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
            this.saveTransformer();
        }

        return true;
    }

    saveTransformer() {/* TODO
        var transformerJson = this.transformerJSON();
        if (!this.isPaste) {
            this.close();
            return;
        }

        if (transformerJson) {
            var transformerDto: savedTransformerDto;
            var transformerObj: transformer;

            try {
                transformerDto = JSON.parse(transformerJson);
                transformerObj = new transformer().initFromSave(transformerDto);
            } catch (e) {
                transformerDto = null;
                transformerObj = null;
                messagePublisher.reportError("Transformer paste failed, invalid JSON.", e);
            }

            if (transformerDto) {
                // Verify there's not a transformer with this name.
                new getSingleTransformerCommand(transformerDto.Transformer.Name, this.db)
                    .execute()
                    .done(() => messagePublisher
                        .reportError("Duplicate transformer name. Change the name and try again."))
                    .fail((xhr: JQueryXHR, status: any, error: string) => {
                        if (xhr.status === ResponseCodes.NotFound) {
                            // Good. No existing transformer with this name. We can proceed saving it.
                            new saveTransformerCommand(transformerObj, this.db)
                                .execute()
                                .done(() => {
                                    router.navigate(appUrl.forEditTransformer(transformerObj.name(), this.db));
                                    this.close();
                                });
                        } else {
                            // Some other error occurred while checking for duplicate transformer. Error out.
                            messagePublisher.reportError("Cannot paste transformer, error occured.", error);
                        }
                    });
            }
        } else {
            this.close();
        }*/
    }

    close() {
        dialog.close(this);
    }
}

export = copyTransformerDialog; 
