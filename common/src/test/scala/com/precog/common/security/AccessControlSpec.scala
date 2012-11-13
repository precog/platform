package com.precog.common
package security

import org.specs2.mutable._

import org.joda.time.DateTime

import scala.collection.mutable

import scalaz._
import scalaz.Id._

class AccessControlSpec extends Specification {

  val apiKeyManager = new InMemoryAPIKeyManager[Id]
  import apiKeyManager._
  
  "access control" should {
    
    "allow user accounts to read/reduce their data on any path" in {
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey

      hasCapability(userAPIKey, Set(ReadPermission(Path("/user"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(ReducePermission(Path("/user"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(ReadPermission(Path("/other"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(ReducePermission(Path("/other"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(ReadPermission(Path("/"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(ReducePermission(Path("/"), Set(userAccountId)))) must beTrue
    }

    "prevent user accounts from reading/reducing others data" in {
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey
      
      val otherAccountId = "other"

      hasCapability(userAPIKey, Set(ReadPermission(Path("/user"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/user"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReadPermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReadPermission(Path("/"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/"), Set(otherAccountId)))) must beFalse
    }
    
    "allow user accounts to write/delete any data under their path" in {
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey

      val otherAccountId = "other"

      hasCapability(userAPIKey, Set(WritePermission(Path("/user"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(DeletePermission(Path("/user"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(WritePermission(Path("/user"), Set(otherAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(DeletePermission(Path("/user"), Set(otherAccountId)))) must beTrue
    }

    "prevent user accounts from writing/deleting any data under another accounts path" in {
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey

      val otherAccountId = "other"

      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(userAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(userAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(otherAccountId)))) must beFalse
    }
    
    "allow user accounts to read/reduce others data via a grant" in {
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey
      
      val otherAccountId = "other"
      val otherAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(otherAccountId, Path(otherAccountId))
      val otherAPIKey = otherAPIKeyRecord.apiKey

      hasCapability(userAPIKey, Set(ReadPermission(Path("/user"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/user"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReadPermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReadPermission(Path("/"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/"), Set(otherAccountId)))) must beFalse
      
      val readReduceOther = Set[Permission](
        ReadPermission(Path("/other"), Set(otherAccountId)),
        ReducePermission(Path("/other"), Set(otherAccountId))
      )
      
      apiKeyManager.deriveAndAddGrant(None, None, otherAPIKey, readReduceOther, userAPIKey).get
      
      hasCapability(userAPIKey, Set(ReadPermission(Path("/user"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/user"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReadPermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(ReducePermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(ReadPermission(Path("/"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/"), Set(otherAccountId)))) must beFalse
    }

    "allow user accounts to write/delete data under another accounts path via a grant" in {
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey

      val otherAccountId = "other"
      val otherAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(otherAccountId, Path(otherAccountId))
      val otherAPIKey = otherAPIKeyRecord.apiKey

      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(userAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(userAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      
      val writeDeleteOther = Set[Permission](
        WritePermission(Path("/other"), Set(userAccountId)),
        DeletePermission(Path("/other"), Set(userAccountId)),
        WritePermission(Path("/other"), Set(otherAccountId)),
        DeletePermission(Path("/other"), Set(otherAccountId))
      )
      
      apiKeyManager.deriveAndAddGrant(None, None, otherAPIKey, writeDeleteOther, userAPIKey).get

      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(userAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(otherAccountId)))) must beTrue
    }
    
    "prevent access via invalid API key" in {
      val invalidAPIKey = "not-there"
      
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey

      hasCapability(invalidAPIKey, Set(ReadPermission(Path("/"), Set()))) must beFalse
      hasCapability(invalidAPIKey, Set(ReducePermission(Path("/"), Set()))) must beFalse
      hasCapability(invalidAPIKey, Set(WritePermission(Path("/"), Set()))) must beFalse
      hasCapability(invalidAPIKey, Set(DeletePermission(Path("/"), Set()))) must beFalse
      
      hasCapability(invalidAPIKey, Set(ReadPermission(Path("/user"), Set(userAccountId)))) must beFalse
      hasCapability(invalidAPIKey, Set(ReducePermission(Path("/user"), Set(userAccountId)))) must beFalse
      hasCapability(invalidAPIKey, Set(WritePermission(Path("/user"), Set(userAccountId)))) must beFalse
      hasCapability(invalidAPIKey, Set(DeletePermission(Path("/user"), Set(userAccountId)))) must beFalse
    }

    "prevent access via a revoked grant" in {
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey
      
      val otherAccountId = "other"
      val otherAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(otherAccountId, Path(otherAccountId))
      val otherAPIKey = otherAPIKeyRecord.apiKey

      val accessOther = Set[Permission](
        ReadPermission(Path("/other"), Set(otherAccountId)),
        ReducePermission(Path("/other"), Set(otherAccountId)),
        WritePermission(Path("/other"), Set()),
        DeletePermission(Path("/other"), Set())
      )
      
      val accessOtherGrant = apiKeyManager.deriveAndAddGrant(None, None, otherAPIKey, accessOther, userAPIKey).get

      hasCapability(userAPIKey, Set(ReadPermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(ReducePermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(otherAccountId)))) must beTrue

      apiKeyManager.deleteGrant(accessOtherGrant)
      
      hasCapability(userAPIKey, Set(ReadPermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(otherAccountId)))) must beFalse
    }

    "prevent access via an expired grant" in {
      val userAccountId = "user"
      val userAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(userAccountId, Path(userAccountId))
      val userAPIKey = userAPIKeyRecord.apiKey
      
      val otherAccountId = "other"
      val otherAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(otherAccountId, Path(otherAccountId))
      val otherAPIKey = otherAPIKeyRecord.apiKey

      val accessOther = Set[Permission](
        ReadPermission(Path("/other"), Set(otherAccountId)),
        ReducePermission(Path("/other"), Set(otherAccountId)),
        WritePermission(Path("/other"), Set()),
        DeletePermission(Path("/other"), Set())
      )
      
      val expiredAccessOtherGrant = apiKeyManager.deriveAndAddGrant(None, None, otherAPIKey, accessOther, userAPIKey, Some(new DateTime().minusYears(1000))).get

      hasCapability(userAPIKey, Set(ReadPermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(ReducePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(WritePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(userAPIKey, Set(DeletePermission(Path("/other"), Set(otherAccountId)))) must beFalse
    }
    
    "prevent access via a grant with a revoked parent" in {
      val user1AccountId = "user1"
      val user1APIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(user1AccountId, Path(user1AccountId))
      val user1APIKey = user1APIKeyRecord.apiKey
      
      val user2AccountId = "user2"
      val user2APIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(user2AccountId, Path(user2AccountId))
      val user2APIKey = user2APIKeyRecord.apiKey
      
      val otherAccountId = "other"
      val otherAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(otherAccountId, Path(otherAccountId))
      val otherAPIKey = otherAPIKeyRecord.apiKey
      
      val accessOther = Set[Permission](
        ReadPermission(Path("/other"), Set(otherAccountId)),
        ReducePermission(Path("/other"), Set(otherAccountId)),
        WritePermission(Path("/other"), Set()),
        DeletePermission(Path("/other"), Set())
      )
      
      val user1AccessOtherGrant = apiKeyManager.deriveAndAddGrant(None, None, otherAPIKey, accessOther, user1APIKey).get
      val user2AccessOtherGrant = apiKeyManager.deriveAndAddGrant(None, None, user1APIKey, accessOther, user2APIKey).get

      hasCapability(user2APIKey, Set(ReadPermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(user2APIKey, Set(ReducePermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(user2APIKey, Set(WritePermission(Path("/other"), Set(otherAccountId)))) must beTrue
      hasCapability(user2APIKey, Set(DeletePermission(Path("/other"), Set(otherAccountId)))) must beTrue

      apiKeyManager.deleteGrant(user1AccessOtherGrant)
      
      hasCapability(user2APIKey, Set(ReadPermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(user2APIKey, Set(ReducePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(user2APIKey, Set(WritePermission(Path("/other"), Set(otherAccountId)))) must beFalse
      hasCapability(user2APIKey, Set(DeletePermission(Path("/other"), Set(otherAccountId)))) must beFalse
    }

    "support addon grants sandboxed to customer paths" in {
      val addOnAccountId = "addon"
      val addOnAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(addOnAccountId, Path(addOnAccountId))
      val addOnAPIKey = addOnAPIKeyRecord.apiKey

      val customer1AccountId = "customer1"
      val customer1APIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(customer1AccountId, Path(customer1AccountId))
      val customer1APIKey = customer1APIKeyRecord.apiKey
      
      val customer2AccountId = "customer2"
      val customer2APIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(customer2AccountId, Path(customer2AccountId))
      val customer2APIKey = customer2APIKeyRecord.apiKey

      val readCustomer1Customer1 = Set[Permission](ReadPermission(Path("/customer1/data"), Set(customer1AccountId)))
      val readCustomer1AddOn = Set[Permission](ReadPermission(Path("/customer1/data"), Set(addOnAccountId)))

      val readCustomer2Customer2 = Set[Permission](ReadPermission(Path("/customer2/data"), Set(customer2AccountId)))
      val readCustomer2AddOn = Set[Permission](ReadPermission(Path("/customer2/data"), Set(addOnAccountId)))
      
      hasCapability(customer1APIKey, readCustomer1Customer1) must beTrue
      hasCapability(customer1APIKey, readCustomer1AddOn) must beFalse
      hasCapability(customer1APIKey, readCustomer2AddOn) must beFalse
      hasCapability(customer2APIKey, readCustomer2Customer2) must beTrue
      hasCapability(customer2APIKey, readCustomer2AddOn) must beFalse
      hasCapability(customer2APIKey, readCustomer1AddOn) must beFalse
      
      val customer1CanRead = apiKeyManager.deriveAndAddGrant(None, None, addOnAPIKey, readCustomer1AddOn, customer1APIKey).get
      val customer2CanRead   = apiKeyManager.deriveAndAddGrant(None, None, addOnAPIKey, readCustomer2AddOn, customer2APIKey).get

      hasCapability(customer1APIKey, readCustomer1Customer1) must beTrue
      hasCapability(customer1APIKey, readCustomer1AddOn) must beTrue
      hasCapability(customer1APIKey, readCustomer2AddOn) must beFalse
      hasCapability(customer2APIKey, readCustomer2Customer2) must beTrue
      hasCapability(customer2APIKey, readCustomer2AddOn) must beTrue
      hasCapability(customer2APIKey, readCustomer1AddOn) must beFalse
    }
    
   "support providers delegating services to addons" in {
      val addOnAccountId = "addon"
      val addOnAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(addOnAccountId, Path(addOnAccountId))
      val addOnAPIKey = addOnAPIKeyRecord.apiKey

      val providerAccountId = "provider"
      val providerAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(providerAccountId, Path(providerAccountId))
      val providerAPIKey = providerAPIKeyRecord.apiKey 

      val customerAccountId = "customer"
      val customerAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(customerAccountId, Path(customerAccountId))
      val customerAPIKey = customerAPIKeyRecord.apiKey 

      val readPerm  = Set[Permission](ReadPermission(Path("/provider/customer/data"), Set(customerAccountId)))
      val writePerm = Set[Permission](WritePermission(Path("/provider/customer/data"), Set(customerAccountId)))
      
      hasCapability(providerAPIKey, readPerm) must beFalse
      hasCapability(providerAPIKey, writePerm) must beTrue
      hasCapability(customerAPIKey, readPerm) must beTrue
      hasCapability(customerAPIKey, writePerm) must beFalse
      hasCapability(addOnAPIKey, readPerm) must beFalse
      hasCapability(addOnAPIKey, writePerm) must beFalse

      val addOnCanWrite = apiKeyManager.deriveAndAddGrant(None, None, providerAPIKey, writePerm, addOnAPIKey).get

      hasCapability(providerAPIKey, readPerm) must beFalse
      hasCapability(providerAPIKey, writePerm) must beTrue
      hasCapability(customerAPIKey, readPerm) must beTrue
      hasCapability(customerAPIKey, writePerm) must beFalse
      hasCapability(addOnAPIKey, readPerm) must beFalse
      hasCapability(addOnAPIKey, writePerm) must beTrue

      apiKeyManager.deleteGrant(addOnCanWrite)
      
      hasCapability(providerAPIKey, readPerm) must beFalse
      hasCapability(providerAPIKey, writePerm) must beTrue
      hasCapability(customerAPIKey, readPerm) must beTrue
      hasCapability(customerAPIKey, writePerm) must beFalse
      hasCapability(addOnAPIKey, readPerm) must beFalse
      hasCapability(addOnAPIKey, writePerm) must beFalse
    }

    "support addons granting revokable access" in {
      val addOnAccountId = "addon"
      val addOnAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(addOnAccountId, Path(addOnAccountId))
      val addOnAPIKey = addOnAPIKeyRecord.apiKey
      
      val customerAccountId = "customer"
      val customerAPIKeyRecord = apiKeyManager.newStandardAPIKeyRecord(customerAccountId, Path(customerAccountId))
      val customerAPIKey = customerAPIKeyRecord.apiKey 

      val addOnPerm = Set(ReadPermission(Path("/addon/public"), Set(addOnAccountId)) : Permission)
        
      hasCapability(addOnAPIKey, addOnPerm) must beTrue
      hasCapability(customerAPIKey, addOnPerm) must beFalse
 
      val derivedGrantId = apiKeyManager.deriveAndAddGrant(None, None, addOnAPIKey, addOnPerm, customerAPIKey).get
        
      hasCapability(addOnAPIKey, addOnPerm) must beTrue
      hasCapability(customerAPIKey, addOnPerm) must beTrue
        
      apiKeyManager.deleteGrant(derivedGrantId)
        
      hasCapability(addOnAPIKey, addOnPerm) must beTrue
      hasCapability(customerAPIKey, addOnPerm) must beFalse
    }
  }
}
