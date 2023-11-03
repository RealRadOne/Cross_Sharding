use rand::distributions::{Distribution, Bernoulli, Uniform};
use rand_distr::Zipf;
use bytes::BufMut as _;
use bytes::BytesMut;
use rand::Rng;
use std::cmp;
use std::collections::HashSet;

const MAX_DEPOSIT: u32 = 100;
const MAX_AMOUNT: u32 = u32::MAX;
const SPLIT_PARTY_SIZE_MIN: u32 = 3;
const SPLIT_PARTY_SIZE_MAX: u32 = 5;

struct SmallBank{
    n_users: u64,
    checking_accounts: Vec<u32>,
    saving_accounts: Vec<u32>,
}

impl SmallBank{
    pub fn new(n_users: u64) -> Self {
        println!("Hello, world, SmallBank!");
        let mut checking_accounts: Vec<u32> = Vec::new();
        let mut saving_accounts: Vec<u32> = Vec::new();

        for user_id in 0..n_users{
            checking_accounts.push(1000);
            saving_accounts.push(1000);
        }

        SmallBank {
            n_users: n_users,
            checking_accounts: checking_accounts,
            saving_accounts: saving_accounts,
        }
    }

    pub fn get_checking_amount(&self, user_id: u32) -> u32{
        let uid: usize =  user_id as usize;
        println!("{} uid in checking balance", uid);
        return self.checking_accounts[uid];
    }

    pub fn get_saving_amount(&self, user_id:u32) -> u32{
        let uid: usize =  user_id as usize;
        println!("{} uid in saving balance", uid);
        return self.saving_accounts[uid];
    }
}

pub struct SmallBankTransactionHandler{
    tx_size: usize,
    n_users: u64,
    skew_factor: f64,
    prob_choose_mtx: f64,
    user_distribution: Zipf<f64>,
    tx_type_distribution: Bernoulli,
    mtx_distribution: Uniform<i32>,
    small_bank: SmallBank,
}

impl SmallBankTransactionHandler{

    pub fn new(tx_size: usize, n_users: u64, skew_factor: f64, prob_choose_mtx: f64) -> Self {
        println!("Hello, world, SmallBankTransactionHandler!");
        let user_distribution = Zipf::new(n_users-1, skew_factor).unwrap(); 
        let tx_type_distribution = Bernoulli::new(prob_choose_mtx).unwrap();
        let mtx_distribution = Uniform::from(0..6);
        let small_bank = SmallBank::new(n_users);

        SmallBankTransactionHandler {
            tx_size: tx_size,
            n_users: n_users,
            prob_choose_mtx: prob_choose_mtx,
            skew_factor: skew_factor,
            user_distribution: user_distribution,
            tx_type_distribution: tx_type_distribution,
            mtx_distribution: mtx_distribution,
            small_bank: small_bank,
        }
    }

    fn _generate_random(&self, min:u32, max: u32) -> u32{
        return rand::thread_rng().gen_range(min..max+1);
        // return rand::thread_rng().gen::<u32>() % max_number;
    }

    fn _sample_user(&self) -> u64{
        return self.user_distribution.sample(&mut rand::thread_rng()) as u64;
    }

    fn _sample_tx_type(&self) -> bool{
        return self.tx_type_distribution.sample(&mut rand::thread_rng());
    }

    fn _sample_mtx(&self) -> i32{
        return self.mtx_distribution.sample(&mut rand::thread_rng());
    }

    fn _generate_transaction(&self, tx_id:u8, sample_tx:bool){
        let mut tx = BytesMut::with_capacity(self.tx_size);

        // Update first byte
        if sample_tx{
            tx.put_u8(0u8); // Sample txs start with 0.
        }
        else{
            tx.put_u8(1u8); // Standard txs start with 1.
        }

        // Update second byte from transaction id
        tx.put_u8(tx_id);

        match tx_id{
            0 => {
                println!("case 0");
                // transaction_savings
                let user_id:u32 = self._sample_user() as u32;
                let amount_to_deposit: u32 = self._generate_random(
                                                0, 
                                                MAX_AMOUNT-self.small_bank.get_saving_amount(user_id)
                                            );

                // Update tx
                tx.put_u32(user_id);
                tx.put_u32(amount_to_deposit);
            },
            1 => {
                println!("case 1");
                // Deposit checking
                let user_id:u32 = self._sample_user() as u32;
                let amount_to_deposit: u32 = self._generate_random(
                                                0, 
                                                MAX_AMOUNT - self.small_bank.get_checking_amount(user_id)
                                            );

                // Update tx
                tx.put_u32(user_id);
                tx.put_u32(amount_to_deposit);
            },
            2 => {
                println!("case 2");
                // write_check
                let user_id:u32 = self._sample_user() as u32;
                let amount_to_withdraw: u32 = self._generate_random(
                                                0, 
                                                self.small_bank.get_checking_amount(user_id)
                                            );

                // Update tx
                tx.put_u32(user_id);
                tx.put_u32(amount_to_withdraw);
            },
            3 => {
                println!("case 3");
                // send_payment
                let from_user_id: u32 = self._sample_user() as u32;
                let to_user_id: u32 = self._sample_user() as u32;
                let amount_to_transfer: u32 = self._generate_random(
                                            0, 
                                            cmp::min(
                                                self.small_bank.get_checking_amount(from_user_id),
                                                MAX_AMOUNT - self.small_bank.get_checking_amount(to_user_id)
                                            )
                                        );
                
                // Update tx
                tx.put_u32(from_user_id);
                tx.put_u32(to_user_id);
                tx.put_u32(amount_to_transfer);
            },
            4 => {
                println!("case 4");
                // Split transaction
                let party_size: u32 = self._generate_random(
                                        SPLIT_PARTY_SIZE_MIN,
                                        SPLIT_PARTY_SIZE_MAX
                                    );
                let n_payors: u32 = self._generate_random(
                                            1,
                                            party_size/2
                                        );
                let n_payees: u32 = party_size - n_payors;
                let mut user_set: HashSet<u32> = HashSet::new();
                while user_set.len() < party_size as usize{
                    user_set.insert(self._sample_user() as u32);
                }
                let mut amount_to_split: u32 = 0;
                let mut count: u32 = 0;
                // update transaction
                tx.put_u32(n_payors);
                tx.put_u32(party_size-n_payors);
                for user_id in user_set{
                    if count<n_payors{
                        tx.put_u32(user_id);
                        let amount = self._generate_random(0,
                                        cmp::min(50,
                                            self.small_bank.get_checking_amount(user_id)
                                        )
                                    );
                        tx.put_u32(amount);
                        amount_to_split += amount;
                    }
                    else{
                        tx.put_u32(amount_to_split/n_payees);
                        if count==party_size-1{
                            tx.put_u32(amount_to_split - ((n_payees-1)*(amount_to_split/n_payees)));
                        }
                    }
                    count += 1;
                }

            },
            5 => {
                println!("case 5");
                // amalgamate
                let user_id: u32 = self._sample_user() as u32;
                // Update tx
                tx.put_u32(user_id);
            },
            _ => {
                println!("case _");
                // Read
                let user_id: u32 = self._sample_user() as u32;
                // Update tx
                tx.put_u32(user_id);
            },
        }
    }

    fn _serialize(&self){

    }

    pub fn get_next_transaction(&self, sample_tx:bool){
        println!("get_next_transaction 0");

        // get transaction id
        let mut tx_id:u8 =  6;
        if self._sample_tx_type()==true{
            tx_id = self._sample_mtx() as u8;
        }

        // Generate transaction
        self._generate_transaction(tx_id, sample_tx);

    }

    // fn print_type_of<T>(_: &T) {
    //     println!("{}", std::any::type_name::<T>())
    // }  

}