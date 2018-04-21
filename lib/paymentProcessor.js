var fs = require('fs');
var async = require('async');
var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);

var logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);

log('info', logSystem, 'Started');

function runInterval(){
    async.waterfall([

        //Get worker keys
        function(callback){
            redisClient.keys(config.coin + ':workers:*', function(error, result) {
                if (error) {
                    log('error', logSystem, 'Error trying to get worker balances from redis %j', [error]);
                    callback(true);
                    return;
                }
                callback(null, result);
            });
        },

        //Get worker balances
        function(keys, callback){
            var redisCommands = keys.map(function(k){
                return ['hget', k, 'balance'];
            });
            redisClient.multi(redisCommands).exec(function(error, replies){
                if (error){
                    log('error', logSystem, 'Error with getting balances from redis %j', [error]);
                    callback(true);
                    return;
                }
                var balances = {};
                for (var i = 0; i < replies.length; i++){
                    var parts = keys[i].split(':');
                    var workerId = parts[parts.length - 1];
                    balances[workerId] = parseInt(replies[i]) || 0

                }
                callback(null, balances);
            });
        },

        //Filter workers under balance threshold for payment
        function(balances, callback){

            var payments = {};
            
            for (var worker in balances){
                var balance = balances[worker];
                if (balance >= config.payments.minPayment){
                    var remainder = balance % config.payments.denomination;
                    var payout = balance - remainder;
                    if (payout < 0) continue;
                    payments[worker] = payout;
                }
            }

            if (Object.keys(payments).length === 0){
                log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
                callback(true);
                
                return false;
            }

            var transferCommands = [];
            var addresses = 0;
            var commandAmount = 0;
            var commandIndex = 0;
			
            for (var worker in payments){
                var amount = parseInt(payments[worker]);
				if(config.payments.maxTransactionAmount && amount + commandAmount > config.payments.maxTransactionAmount) {
		            amount = config.payments.maxTransactionAmount - commandAmount;
	            }
				
				if(!transferCommands[commandIndex]) {
					transferCommands[commandIndex] = {
						redis: [],
						rpc: {
                            'any_spend_address': true,
                            'change_address': config.poolServer.poolAddress,
                            'fee_per_coin': config.payments.transferFeePerCoin,
                            'transaction': {
                                'amount': 0,
                                'anonymity': 0,
                                'fee': 0,
                                'transfers':[],
                                'unlock_time': 0
                            }
						}
					};
				}
				
                transferCommands[commandIndex].rpc.transaction.transfers.push({amount: amount, address: worker});
                transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -amount]);
                transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid', amount]);
                transferCommands[commandIndex].rpc.transaction.amount += amount;

                addresses++;
				commandAmount += amount;
                
                if (addresses >= config.payments.maxAddresses || ( config.payments.maxTransactionAmount && commandAmount >= config.payments.maxTransactionAmount)) {
                    commandIndex++;
                    addresses = 0;
					commandAmount = 0;
                }
            }

            var timeOffset = 0;

            async.filter(transferCommands, function(transferCmd, cback){
                apiInterfaces.rpcWallet('create_transaction', transferCmd.rpc, function(error, transaction){
                    if (error){
                        log('error', logSystem, 'Failed to create transaction %j', [error]);
                        cback();
                        
                        return false;
                    }

                    apiInterfaces.rpcWallet('send_transaction', { 'binary_transaction': transaction.binary_transaction }, function(error, result){
                        if (error){
                            log('error', logSystem, 'Error with transfer RPC request to wallet daemon %j', [error]);
                            log('error', logSystem, 'Payments failed to send to %j', transferCmd.rpc.transaction.transfers);
                            cback();
                            
                            return false;
                        }
                        
                        var now = (timeOffset++) + Date.now() / 1000 | 0;

                        transferCmd.redis.push(['zadd', config.coin + ':payments:all', now, [
                            transaction.hash,
                            transferCmd.rpc.transaction.amount,
                            transferCmd.rpc.transaction.fee,
                            Object.keys(transferCmd.rpc.transaction.transfers).length
                        ].join(':')]);


                        for (var i = 0; i < transferCmd.rpc.transaction.transfers.length; i++){
                            var destination = transferCmd.rpc.transaction.transfers[i];
                            transferCmd.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
                                transaction.hash,
                                destination.amount,
                                transferCmd.rpc.transaction.amount,
                                transferCmd.rpc.transaction.fee
                            ].join(':')]);
                        }

                        log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
                        
                        redisClient.multi(transferCmd.redis).exec(function(error, replies){
                            if (error){
                                log('error', logSystem, 'Super critical error! Payments sent yet failing to update balance in redis, double payouts likely to happen %j', [error]);
                                log('error', logSystem, 'Double payments likely to be sent to %j', transferCmd.rpc.transaction.transfers);
                                cback();
                                
                                return false;
                            }
                            cback(null, true);
                        });
                    });
                });
            }, function(err, succeeded){
                var failedAmount = transferCommands.length - succeeded.length;
                log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed', [succeeded.length, failedAmount]);
                callback(null);
            });

        }

    ], function(error, result){
        setTimeout(runInterval, config.payments.interval * 1000);
    });
}

runInterval();
