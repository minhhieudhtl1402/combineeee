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
        //        exampleCurrentValueSubject()
        //        exampleCollectOperator()
        //        exampleMapOperator()
        //        exampleFlatMapOperator()
//        exampleOfEmpty()
        exampleOfCompactMap()
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
        
        subject
            .print()
            .sink { value in
                print("current value", value)
            }.store(in: &subscriptions)
        
        
        let typeErasurePublisher = subject.eraseToAnyPublisher()
    }
    
    func exampleCollectOperator() {
        var subscriptions = Set<AnyCancellable>()
        let subject =  CurrentValueSubject<Int, Never>(0)
        
        subject
            .print()
            .collect(3)
            .sink { value in
                print("current value", value)
            }.store(in: &subscriptions)
        subject.send(1)
        subject.send(2)
        subject.send(3)
        subject.send(4)
        subject.send(5)
        subject.send(6)
        subject.send(7)
        subject.send(completion: .finished)
    }
    
    func exampleMapOperator() {
        var subscriptions = Set<AnyCancellable>()
        let subject = CurrentValueSubject<Person, Never>(.init(name: "vmhieu", age: 25, salary: 10000))
        
        subject
            .print()
        //            .map(\.name)
            .map(\.name, \.salary)
            .sink { value in
                print("current value", value)
            }.store(in: &subscriptions)
        subject.send(.init(name: "vmhieu", age: 25, salary: 12000))
        subject.send(.init(name: "vmhieu12", age: 26, salary: 130000))
        subject.send(.init(name: "vmhi32eu", age: 27, salary: 14000))
        subject.send(.init(name: "vmhi4565eu", age: 28, salary: 15000))
        subject.send(.init(name: "v67mhieu", age: 29, salary: 16000))
        subject.send(.init(name: "vmhi779eu", age: 30, salary: 18000))
        subject.send(.init(name: "vmh90ieu", age: 31, salary: 10800))
        subject.send(completion: .finished)
    }
    
    func exampleFlatMapOperator() {
        struct FuncChatter {
            let name: String
            let chatter: CurrentValueSubject<String, Never>
        }
        var subscriptions = Set<AnyCancellable>()
        
        let hieuChatter = FuncChatter(name: "hieu", chatter: CurrentValueSubject<String, Never>(.init("Hieu start")))
        let pLinhChatter = FuncChatter(name: "PLinh", chatter: CurrentValueSubject<String, Never>(.init("PLinh start")))
        
        let chatterSubject = CurrentValueSubject<FuncChatter, Never>(hieuChatter)
        chatterSubject.flatMap(maxPublishers: .max(2)) { funcChatter in
            funcChatter.chatter
        }.sink { message in
            print("message",message)
        }.store(in: &subscriptions)
        chatterSubject.send(pLinhChatter)
        hieuChatter.chatter.send("Hello im hieu")
        pLinhChatter.chatter.send("Hello Hieu,im PLinh")
        hieuChatter.chatter.send("how are you PLinh?")
        pLinhChatter.chatter.send("im fine")
    }
    
    func exampleOfEmpty() {
        
        var subscriptions = Set<AnyCancellable>()
        let empty = Empty<Int, Never>()
        // 2
        empty
            .replaceEmpty(with: 36)
            .sink(receiveCompletion: { print($0) },
                  receiveValue: { print($0) })
            .store(in: &subscriptions)
    }
    
    func exampleOfCompactMap() {
        var subscriptions = Set<AnyCancellable>()
        let strings = ["a", "1.24", "3", "def", "45", "0.23"].publisher
        
        // 2
        strings
            .compactMap { Float($0) }
            .sink(receiveValue: {
                // 3
                print($0)
            })
            .store(in: &subscriptions)
    }
}

struct Person {
    let name: String
    let age: Int
    let salary: Double
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
