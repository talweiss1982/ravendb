/// <reference path="../../typings/tsd.d.ts"/>

import virtualGrid = require("widgets/virtualGrid/virtualGrid");

class extensions {
    static install() {
        extensions.installObservableExtensions();
        extensions.installStorageExtension();
        extensions.installBindingHandlers();
        extensions.installJqueryExtensions();
        extensions.configureValidation();

        virtualGrid.install();

        // Want Intellisense for your extensions?
        // Go to extensionInterfaces.ts and add the function signature there.
    }

    private static configureValidation() {

        (ko.validation.rules as any)['base64'] = {
            validator: (val: string) => {
                const base64regex = /^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/;
                return !val || base64regex.test(val);
            },
            message: 'Invaild base64 string.'
        };       

        (ko.validation.rules as any)['validJson'] = {
            validator: (text: string) => {
                let isValidJson = false;
                try {
                    JSON.parse(text);
                    isValidJson = true;
                }
                catch (e) { }
                return isValidJson;
            },
            message: 'Invalid json format.'
        };

        ko.validation.init({
            errorElementClass: 'has-error',
            errorMessageClass: 'help-block',
            decorateInputElement: true
        });
    }

    private static installObservableExtensions() {
        const subscribableFn: any = ko.subscribable.fn;

        subscribableFn.distinctUntilChanged = function () {
            const observable: KnockoutObservable<any> = this;
            const matches = ko.observable();
            let lastMatch = observable();
            observable.subscribe(val => {
                if (val !== lastMatch) {
                    lastMatch = val;
                    matches(val);
                }
            });
            return matches;
        };

        subscribableFn.throttle = function (throttleTimeMs: number) {
            const observable = this;
            return ko.pureComputed(() => observable()).extend({ throttle: throttleTimeMs });
        };

        subscribableFn.toggle = function () {
            const observable: KnockoutObservable<boolean> = this;
            observable(!observable());
            return observable;
        };
    }

    private static installStorageExtension() {
        Storage.prototype.getObject = function (key) {
            const value = this.getItem(key);
            return value && JSON.parse(value);
        }

        Storage.prototype.setObject = function (key, value) {
            this.setItem(key, ko.toJSON(value));
        }
    }

    private static installBindingHandlers() {
      
        ko.bindingHandlers["scrollTo"] = {
            update: (element: any, valueAccessor: KnockoutObservable<boolean>) => {
                if (valueAccessor()) {
                  
                    const $container = $("#page-host-root");
                    const scrollTop = $container.scrollTop();
                    const scrollBottom = scrollTop + $container.height();

                    const $element = $(element);
                    const elementTop = $element.position().top;
                    const elementBottom = elementTop + $element.height();

                    // Scroll vertically only if element is outside of viewport 
                    if ((elementTop < scrollTop) || (elementBottom > scrollBottom)){
                        $container.scrollTop(elementTop);
                    } 
                }
            }
        };

        ko.bindingHandlers["collapse"] = {
            init: (element: any, valueAccessor: KnockoutObservable<boolean>) => {
                var value = valueAccessor();
                var valueUnwrapped = ko.unwrap(value);
                var $element = $(element);
                $element
                    .addClass('collapse')
                    .collapse({
                        toggle: valueUnwrapped
                    });
            },

            update: (element: any, valueAccessor: KnockoutObservable<boolean>) => {
                var value = valueAccessor();
                var valueUnwrapped = ko.unwrap(value);
                $(element).collapse(valueUnwrapped ? "show" : "hide");
            }
        };

        ko.bindingHandlers["numericValue"] = {
            init: (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) => {
                var underlyingObservable = valueAccessor();
                var interceptor = ko.computed({
                    read: underlyingObservable,
                    write: (value: any) => {
                        if (value && !isNaN(value)) {
                            underlyingObservable(parseFloat(value));
                        } else {
                            underlyingObservable(undefined);
                        }
                    },
                    disposeWhenNodeIsRemoved: element
                });
                ko.bindingHandlers.value.init(element, () => interceptor, allBindingsAccessor, viewModel, bindingContext);
            },
            update: ko.bindingHandlers.value.update
        };

        ko.bindingHandlers["customValidity"] = {
            update: (element, valueAccessor) => {
                var errorMessage = ko.unwrap(valueAccessor()); //unwrap to get subscription
                element.setCustomValidity(errorMessage);
            }
        };

        ko.bindingHandlers["dropdownPanel"] = {
            init: (element) => {
                $(element).on('click', e => {
                    const $target = $(e.target);

                    const clickedOnClose = !!$target.closest(".close-panel").length;
                    if (clickedOnClose) {
                        const $dropdownParent = $target.closest(".dropdown-menu").parent();
                        $dropdownParent.removeClass('open');
                    } else {
                        const $button = $target.closest(".dropdown-toggle");
                        const $dropdown = $button.next(".dropdown-menu");
                        if ($dropdown.length && $dropdown[0] !== element) {
                            if (!$button.is(":disabled")) {
                                const $parent = $dropdown.parent();
                                $parent.toggleClass('open');
                            }
                        } else {
                            // close any child dropdown
                            $(".dropdown", element).each((idx, elem) => {
                                $(elem).removeClass('open');
                            });
                        }

                        e.stopPropagation();
                    }
                });
            }
        }

        ko.bindingHandlers["bsChecked"] = {
            init: (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) => {
                var value = valueAccessor();
                var newValueAccessor = () => {
                    return {
                        change() {
                            value(element.value);
                        }
                    }
                };
                if ($(element).val() === ko.unwrap(valueAccessor())) {
                    $(element).closest(".btn").button("toggle");
                }
                ko.bindingHandlers.event.init(element, newValueAccessor, allBindingsAccessor, viewModel, bindingContext);
            }
        };

        var key = "_my_init_key_";
        ko.bindingHandlers["updateHighlighting"] = {
            init: (element) => {
                ko.utils.domData.set(element, key, true);
            },
            update(element, valueAccessor, allBindings, viewModel, bindingContext) {
                const value = valueAccessor();
                const isInit = ko.utils.domData.get(element, key);
                const data = ko.dataFor(element);
                const skip = !!data.isAllDocuments || !!data.isSystemDocuments || !!data.isAllGroupsGroup;
                if (skip === false && isInit === false) {
                    $($(element).parents("li")).highlight();
                } else {
                    ko.utils.domData.set(element, key, false);
                }
            }
        };

        ko.bindingHandlers["checkboxTriple"] = {
            update(element, valueAccessor, allBindings, viewModel, bindingContext) {
                const checkboxValue: checkbox = ko.unwrap(valueAccessor());
                switch (checkboxValue) {
                case checkbox.Checked:
                    element.checked = true;
                    element.readOnly = false;
                    element.indeterminate = false;
                    break;
                case checkbox.SomeChecked:
                    element.readOnly = true;
                    element.indeterminate = true;
                    element.checked = false;
                    break;
                case checkbox.UnChecked:
                    element.checked = false;
                    element.readOnly = false;
                    element.indeterminate = false;
                    break;
                }
            }
        };
    }

    static installJqueryExtensions() {
        //TODO do we need it?
        jQuery.fn.highlight = function() {
            $(this).each(function() {
                const el = $(this);
                el.before("<div/>");
                el.prev()
                    .width(el.width())
                    .height(el.height())
                    .css({
                        "position": "absolute",
                        "background-color": "#ffff99",
                        "opacity": ".9"
                    })
                    .fadeOut(1500);
            });
        }
    }
}

export = extensions;
