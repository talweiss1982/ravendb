/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import abstractWebSocketClient = require("common/abstractWebSocketClient");

abstract class eventsWebSocketClient<T> extends abstractWebSocketClient<T> {

    private static messageWasShownOnce: boolean = false;

    private sentMessages: chagesApiConfigureRequestDto[] = [];
   
    protected constructor(protected db: database) {
        super(db);
    }

    protected get autoReconnect() {
        return true;
    }

    protected reconnect() {
        //send changes connection args after reconnecting
        this.sentMessages.forEach(args => this.send(args.Command, args.Param, false));

        if (eventsWebSocketClient.messageWasShownOnce) {
            messagePublisher.reportSuccess("Successfully reconnected to changes stream!");
            eventsWebSocketClient.messageWasShownOnce = false;
        }
    }

    protected onError(e: Event) {
        if (eventsWebSocketClient.messageWasShownOnce === false && !this.disposed) {
            messagePublisher.reportError("Changes stream was disconnected!", "Retrying connection shortly.");
            eventsWebSocketClient.messageWasShownOnce = true;
        }
    }

    protected send(command: string, value?: string, needToSaveSentMessages: boolean = true) {
        this.connectToWebSocketTask.done(() => {
            const args: chagesApiConfigureRequestDto = {
                Command: command
            };
            if (value !== undefined) {
                args.Param = value;
            }

            const payload = JSON.stringify(args, null, 2);

            if (!this.closingOrClosed() || !this.isUnwatchCommand(command)) {
                this.webSocket.send(payload);
            }
                
            this.saveSentMessages(needToSaveSentMessages, command, args);
        });
    }

    private isUnwatchCommand(command: string) {
        return command.slice(0, 2) === "un";
    }

    private saveSentMessages(needToSaveSentMessages: boolean, command: string, args: chagesApiConfigureRequestDto) {
        if (needToSaveSentMessages) {
            if (this.isUnwatchCommand(command)) {
                const commandName = command.slice(2, command.length);
                this.sentMessages = this.sentMessages.filter(msg => msg.Command !== commandName);
            } else {
                this.sentMessages.push(args);
            }
        }
    }

    protected fireEvents<T>(events: Array<any>, param: T, filter: (element: T) => boolean) {
        for (let i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

}

export = eventsWebSocketClient;

