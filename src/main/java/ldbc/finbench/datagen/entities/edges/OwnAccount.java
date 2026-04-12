/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class OwnAccount implements DynamicActivity, Serializable {
    private final long ownerId;
    private final PersonOrCompany ownerType;
    private final long accountId;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String comment;

    public OwnAccount(long ownerId, PersonOrCompany ownerType, long accountId, long creationDate, long deletionDate,
                      boolean isExplicitlyDeleted, String comment) {
        this.ownerId = ownerId;
        this.ownerType = ownerType;
        this.accountId = accountId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.comment = comment;
    }

    public static void createOwnAccount(RandomGeneratorFarm farm, Person person, Account account,
                                         long creationDate) {
        account.setOwnerType(PersonOrCompany.PERSON);
        account.setPersonId(person.getPersonId());
        person.addAccount(account);

        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        OwnAccount ownAccount = new OwnAccount(person.getPersonId(),
                                                PersonOrCompany.PERSON,
                                                account.getAccountId(),
                                                creationDate,
                                                account.getDeletionDate(),
                                                account.isExplicitlyDeleted(),
                                                comment);
        person.getOwnAccounts().add(ownAccount);
    }

    public static void createOwnAccount(RandomGeneratorFarm farm, Company company, Account account,
                                         long creationDate) {
        account.setOwnerType(PersonOrCompany.COMPANY);
        account.setCompanyId(company.getCompanyId());
        company.addAccount(account);

        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));
        OwnAccount ownAccount = new OwnAccount(company.getCompanyId(),
                                                PersonOrCompany.COMPANY,
                                                account.getAccountId(),
                                                creationDate,
                                                account.getDeletionDate(),
                                                account.isExplicitlyDeleted(),
                                                comment);
        company.getOwnAccounts().add(ownAccount);
    }

    public long getOwnerId() {
        return ownerId;
    }

    public PersonOrCompany getOwnerType() {
        return ownerType;
    }

    public long getAccountId() {
        return accountId;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public String getComment() {
        return comment;
    }
}
