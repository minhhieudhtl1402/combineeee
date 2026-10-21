//
//  ViewController.swift
//  Combineee
//
//  Created by Vu Minh Hieu on 21/10/26.
//

import UIKit
import Combine
enum MyError: Error {
    case test
}
class ViewController: UIViewController {
    
    override func viewDidLoad() {
        super.viewDidLoad()
        // Do any additional setup after loading the view.
//        let publisher = StringPublisher(value: "Hello Combine")
//        let subscriber = StringSubscriber()
//
//        publisher.subscribe(subscriber)
//        customSubscriber()
//        examplePassthroughSubject()
    }
    
    
    func customSubscriber() {
        let publisher = (1...6).publisher
        
        final class IntSubscriber: Subscriber {
            typealias Input = Int
            
            typealias Failure = Never
            
            func receive(subscription: any Subscription) {
//                subscription.request(.max(3))
                subscription.request(.unlimited)
//                subscription.request(.none)
            }
            
            func receive(_ input: Int) -> Subscribers.Demand {
                print("received value", input)
                return .none
            }
            
            func receive(completion: Subscribers.Completion<Never>) {
                print("received completion",completion)
            }
        }
        
        let subscriber = IntSubscriber()
        publisher.subscribe(subscriber)
    }
    
    func exampleFuture() {
        func futureIncrement(
            integer: Int,
            afterDelay delay: TimeInterval
        ) -> Future<Int, Never> {
            return Future<Int, Never> { promise in
                DispatchQueue.global().asyncAfter(deadline: .now() + delay) {
                    promise(.success(integer + 1))
                }
            }
        }
    }
    
    func examplePassthroughSubject() {
        
        var cancellables = Set<AnyCancellable>()
        let subject = PassthroughSubject<String, MyError>()
        subject.sink { completion in
            print("received completion",completion)
        } receiveValue: { value in
            print("value")
        }.store(in: &cancellables)
        subject.send("mot")
        subject.send("hau")
        subject.send("ba")
        subject.send(completion: .failure(.test))
        subject.send("bon")
    }
    
    func exampleCurrentValueSubject() {
        var subscriptions = Set<AnyCancellable>()
        
        let subject = CurrentValueSubject<Int, Never>(0)
        
        subject.sink { value in
            print("current value", value)
        }.store(in: &subscriptions)
    }
}

struct StringPublisher: Publisher {
    typealias Output = String
    typealias Failure = Never

    let value: String

    func receive<S>(subscriber: S)
    where S: Subscriber,
          S.Input == String,
          S.Failure == Never {

        print("Publisher: /*nhận*/ được yêu cầu subscribe")

        let subscription = StringSubscription(
            subscriber: subscriber,
            value: value
        )

        subscriber.receive(subscription: subscription)
    }
}


class StringSubscription<S: Subscriber>: Subscription
where S.Input == String, S.Failure == Never {
    private var subscriber: S?
    private let value: String
    private var isCompleted = false
    
    init(subscriber: S, value: String) {
        self.subscriber = subscriber
        self.value = value
    }
    
    func request(_ demand: Subscribers.Demand) {
        guard demand > .none,
              let subscriber,
              !isCompleted else { return }
        
        print("Subscription: bắt đầu gửi dữ liệu")
        _ = subscriber.receive(value)
        isCompleted = true
        subscriber.receive(completion: .finished)
        
        self.subscriber = nil
    }
    
    func cancel() {
            subscriber = nil
            isCompleted = true
            print("Subscription: cancelled")
        }
}


struct StringSubscriber: Subscriber {
    typealias Input = String
    typealias Failure = Never

    // Thêm phần này
    var combineIdentifier: CombineIdentifier {
        CombineIdentifier()
    }

    func receive(subscription: Subscription) {
        print("Subscriber: nhận Subscription")
        subscription.request(.unlimited)
    }

    func receive(_ input: String) -> Subscribers.Demand {
        print("Subscriber: nhận value =", input)
        return .none
    }

    func receive(completion: Subscribers.Completion<Never>) {
        print("Subscriber: nhận completion =", completion)
    }
}
