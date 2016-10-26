<?php

namespace PhpScotland2016\Demo\Service\Impls\Rmq;

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Connection\AMQPStreamConnection;

use PhpScotland2016\Demo\Service\Impls\DemoServiceLocal;
use PhpScotland2016\Demo\Service\Interfaces\DemoServiceRequest;
use PhpScotland2016\Demo\Service\Interfaces\DemoServiceResponse;

class DemoServiceRmqConsumer
{
	protected $_context = null;
	protected $_push = null;
	protected $_run  = true;
	protected $_amqp_conn = null;
	protected $_amqp_chan = null;

	public function __construct(/* ToDo, DI here. PRs gladly accepted :) */) {
		$this->connect();
	}

	public function execute() {
		$this->_amqp_chan->basic_consume($_ENV["RMQ_QUEUE"],
			false, // consumer_tag
			false, // no_local
			false, // no_ack
			false, // exclusive 
			false, // nowait
			[$this, "callback"]
		);
		$this->log("Entering AMQP Lib event loop");
		while($this->_run && count($this->_amqp_chan->callbacks)) {
			usleep(10);
			$this->_amqp_chan->wait();
		}
		$this->_amqp_chan->close();
		$this->_amqp_conn->close();
		$this->log("Terminating");
	}

	public function callback(AMQPMessage $msg) {
		$json = $msg->getBody();
		if(is_string($json) && !empty($json)) {
			$this->log("RX: ". $json);
			$request = new DemoServiceRequest($json);
			$response = $this->handleRequest($request);
			$this->send($response);
		}
		$deliver_tag = $msg->delivery_info["delivery_tag"];
		$channel = $msg->delivery_info["channel"];
		$channel->basic_ack($deliver_tag);
	}

	private function handleRequest(DemoServiceRequest $request) {
		$service = new DemoServiceLocal;
		return $service->handleRequest($request);
	}

	private function send(DemoServiceResponse $response) {
		$this->log("TX:".$response->getJson());
		$this->_push->send($response->getJson(), \ZMQ::MODE_NOBLOCK);
	}

	private function log($in) {
		if(isset($_ENV["VERBOSE"]) && (int)$_ENV["VERBOSE"] == 1) {
			error_log($in);
		}
	}

	private function connect() {
		$this->_context = new \ZMQContext(1, true);
		$conn = "tcp://" .  $_ENV["CROSSBAR_HOST"] .":". $_ENV["CROSSBAR_ZMQ_PULL_PORT"];
		$this->log("Connecting to $conn");
		$this->_push = $this->_context->getSocket(\ZMQ::SOCKET_PUSH, null);
		$this->_push->connect($conn);
	
		$this->log("Connecting to RabbitMQ server: ".$_ENV["RMQ_HOST"].':'.$_ENV["RMQ_PORT"]);
		$rmq_ready = false;
		while(!$rmq_ready) {
			try {
				$this->_amqp_conn = new AMQPStreamConnection(
					$_ENV["RMQ_HOST"],
					$_ENV["RMQ_PORT"],
					$_ENV["RMQ_USER"],
					$_ENV["RMQ_PASS"]
				);
				$rmq_ready = true;
			}
			catch(\Exception $e) {
				$this->log("   not ready for us, sleeping...");
				sleep(3);
			}
		}
		$this->log("Connected");

		$this->_amqp_chan = $this->_amqp_conn->channel();
		$this->_amqp_chan->queue_declare($_ENV["RMQ_QUEUE"],
			false, // passive
			false, // durable
			false, // exclusive
			true   // auto_delete
		);
		$this->_amqp_chan->basic_qos(0, 10, true);

		if(extension_loaded("pcntl")) {
			pcntl_signal(SIGTERM, function($signo) {
				$this->_run = false;
			});
		}
	}

}

